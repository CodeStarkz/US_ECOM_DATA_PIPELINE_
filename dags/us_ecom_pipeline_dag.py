from airflow.decorators import dag, task
from pendulum import datetime
import os


@dag(
    dag_id="us_ecom_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    is_paused_upon_creation=True,
    default_args={
        # Using a list for the email field as Airflow expects list of recievers
        'email': ['20mecp01@iiitdmj.ac.in','abhirajpoot01011998@gmail.com'],
        'email_on_failure': True,
        'email_on_retry': False,
    }
)
def us_ecom_pipeline():
    @task
    def check_for_file_from_Retailer_to_organisation(**kwargs):
        input_file_name="data.csv"
        import pandas as pd
        input_path_directory = "/opt/airflow/data/"
        path = input_path_directory + input_file_name
        pd.read_csv(path, encoding="latin1")  # Just checking if it exists/readable
        kwargs['ti'].xcom_push(key="Retailer_file_path_name", value=path)
        kwargs["ti"].xcom_push(key="task_status",value="Completed")
        return "File found"

    @task
    def move_file_from_data_to_intermediate_storage(**kwargs):
        import shutil
        ti = kwargs['ti']
        src = ti.xcom_pull(key="Retailer_file_path_name",
                           task_ids="check_for_file_from_Retailer_to_organisation")
        dest_path = "/opt/airflow/Intermediat_Storage/"
        dest_file_name="data.csv"
        dest= dest_path + dest_file_name
        ti.xcom_push(key="destination_path_file_name", value=dest)
        shutil.move(src, dest)
        kwargs["ti"].xcom_push(key="task_status", value="Completed")
        return "File moved"

    @task
    def transform_data(**kwargs):
        import pandas as pd
        ti = kwargs['ti']
        src = ti.xcom_pull(key="destination_path_file_name",
                           task_ids="move_file_from_data_to_intermediate_storage")
        df = pd.read_csv(src, encoding="latin1")

        # Transformation Logic
        df["InvoiceNo"] = df["InvoiceNo"].astype("int64", errors="ignore")
        df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])

        transformed_file_name="data_transformed.csv"
        transformed_file_save_directory="/opt/airflow/S3_Blob_Storage/"
        out_path =  transformed_file_save_directory + transformed_file_name
        df.to_csv(out_path, index=False)
        os.remove(src)
        ti.xcom_push(key="transformed_path_file_name", value=out_path)
        kwargs["ti"].xcom_push(key="task_status", value="Completed")
        return "Data transformed"

    @task
    def load_data_into_warehouse_db(**kwargs):
        import pandas as pd
        from sqlalchemy import create_engine
        engine = create_engine("mysql+mysqldb://airflow:airflow@mysql:3306/usdb")
        path = kwargs['ti'].xcom_pull(key="transformed_path_file_name",
                                      task_ids="transform_data")
        df = pd.read_csv(path, encoding="latin1")
        df.to_sql("us_ecom_data", con=engine,
                  if_exists="replace", index=False)
        kwargs["ti"].xcom_push(key="task_status", value="Completed")
        return "Loaded"

    @task
    def move_file_from_intermediate_to_backup(**kwargs):
        import shutil
        src = kwargs['ti'].xcom_pull(key="transformed_path_file_name",
                                     task_ids="transform_data")
        dest = "/opt/airflow/Back_Up_Cold_Storage/data_transformed.csv"
        shutil.move(src, dest)
        kwargs["ti"].xcom_push(key="task_status", value="Completed")
        return "Backup complete"
        return ""

    @task
    def send_status_email(**kwargs):
        import os  # Added missing import
        from airflow.utils.email import send_email

        ti = kwargs['ti']
        # Verify the key "task_status" matches what you pushed in the previous task
        last_task_status = ti.xcom_pull(task_ids="load_data_into_warehouse_db", key="task_status")

        if last_task_status == "Completed":
            send_email(
                to=os.getenv("success_report_email_id"),
                subject=f"Success Report: {last_task_status}",
                html_content="Pipeline Successfully Executed, No error detected"
            )
        else:
            send_email(
                to=os.getenv("failure_report_email_id"),
                subject="FAILURE ALERT!!!",
                html_content="Failure Detected, Please check the dashboard and logs for more detailed information"
            )

        return "Mail sent"

    # 3. Setting up the pipeline flow correctly
    step1 = check_for_file_from_Retailer_to_organisation()
    step2 = move_file_from_data_to_intermediate_storage()
    step3 = transform_data()
    step4 = load_data_into_warehouse_db()
    step5 = move_file_from_intermediate_to_backup()
    step6 = send_status_email()

    # Link the tasks
    step1 >> step2 >> step3 >> step4 >> step5 >> step6


us_ecom_pipeline()
