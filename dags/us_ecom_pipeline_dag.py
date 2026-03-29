from airflow.decorators import dag, task
from pendulum import datetime
import os
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.utils.email import send_email


# 1. Define the failure callback function outside the DAG
def notify_failure(context):
    subject = f"FAILED: Task {context['task_instance'].task_id} in {context['dag'].dag_id}"
    html_content = f"The pipeline failed at step: {context['task_instance'].task_id}. Check Airflow logs."
    # Use the email defined in your .env or a hardcoded one
    send_email(to='your-email@gmail.com', subject=subject, html_content=html_content)


@dag(
    dag_id="us_ecom_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    is_paused_upon_creation=True,
    default_args={
        # Using a list for the email field as Airflow expects
        'email': ['20mecp01@iiitdmj.ac.in','abhirajpoot01011998@gmail.com'],
        'email_on_failure': True,
        'email_on_retry': False,
    },
    on_failure_callback=notify_failure
)
def us_ecom_pipeline():
    @task
    def check_for_file_from_Retailer_to_organisation(**kwargs):
        import pandas as pd
        path = "/opt/airflow/data/data.csv"
        pd.read_csv(path, encoding="latin1")  # Just checking if it exists/readable
        kwargs['ti'].xcom_push(key="Retailer_file_path_name", value=path)
        return "File found"

    @task
    def move_file_from_data_to_intermediate_storage(**kwargs):
        import shutil
        ti = kwargs['ti']
        src = ti.xcom_pull(key="Retailer_file_path_name", task_ids="check_for_file_from_Retailer_to_organisation")
        dest = "/opt/airflow/Intermediat_Storage/data.csv"
        ti.xcom_push(key="destination_path_file_name", value=dest)
        shutil.move(src, dest)
        return "File moved"

    @task
    def transform_data(**kwargs):
        import pandas as pd
        ti = kwargs['ti']
        src = ti.xcom_pull(key="destination_path_file_name", task_ids="move_file_from_data_to_intermediate_storage")
        df = pd.read_csv(src, encoding="latin1")

        # Transformation Logic
        df["InvoiceNo"] = df["InvoiceNo"].astype("int64", errors="ignore")
        df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])

        out_path = "/opt/airflow/S3_Blob_Storage/data_transformed.csv"
        df.to_csv(out_path, index=False)
        os.remove(src)
        ti.xcom_push(key="transformed_path_file_name", value=out_path)
        return "Data transformed"

    @task
    def load_data_into_warehouse_db(**kwargs):
        import pandas as pd
        from sqlalchemy import create_engine
        engine = create_engine("mysql+mysqldb://airflow:airflow@mysql:3306/usdb")
        path = kwargs['ti'].xcom_pull(key="transformed_path_file_name", task_ids="transform_data")
        df = pd.read_csv(path, encoding="latin1")
        df.to_sql("us_ecom_data", con=engine, if_exists="replace", index=False)
        return "Loaded"

    @task
    def move_file_from_intermediate_to_backup(**kwargs):
        import shutil
        src = kwargs['ti'].xcom_pull(key="transformed_path_file_name", task_ids="transform_data")
        dest = "/opt/airflow/Back_Up_Cold_Storage/data_transformed.csv"
        shutil.move(src, dest)
        return "Backup complete"

    # 2. Define EmailOperator as a STANDALONE task (not a @task function)
    send_success_email = EmailOperator(
        task_id='send_success_notification',
        to=['20mecp01@iiitdmj.ac.in','abhirajpoot01011998@gmail.com'],
        subject='ETL Pipeline Success: {{ dag.dag_id }}',
        html_content="""
            <h3>Pipeline Run Successful</h3>
            <p><b>DAG:</b> {{ dag.dag_id }}</p>
            <p><b>Execution Date:</b> {{ ds }}</p>
        """
    )

    # 3. Setting up the pipeline flow correctly
    step1 = check_for_file_from_Retailer_to_organisation()
    step2 = move_file_from_data_to_intermediate_storage()
    step3 = transform_data()
    step4 = load_data_into_warehouse_db()
    step5 = move_file_from_intermediate_to_backup()

    # Link the tasks
    step1 >> step2 >> step3 >> step4 >> step5 >> send_success_email


us_ecom_pipeline()
