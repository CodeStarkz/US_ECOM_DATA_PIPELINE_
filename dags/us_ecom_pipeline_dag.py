from airflow.decorators import dag, task
from pendulum import datetime


@dag(
    dag_id="us_ecom_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    is_paused_upon_creation=True
)

def us_ecom_pipeline():

    @task
    def check_for_file_from_Retailer_to_organisation(**kwargs):
        import os
        import pandas as pd
        Retailer_file_path = "/opt/airflow/data"
        Retailer_file_name = "data.csv"
        Retailer_file_path_name = os.path.join(Retailer_file_path, Retailer_file_name)
        df = pd.read_csv(Retailer_file_path_name,encoding="latin1")
        ti=kwargs['ti']
        ti.xcom_push(key="Retailer_file_path_name", value=Retailer_file_path_name)
        return "File found and Ready to move to next step"


    @task
    def move_file_from_data_to_intermediate_storage(**kwargs):
        import os
        import shutil
        ti=kwargs['ti']
        Retailer_file_path_name = ti.xcom_pull(key="Retailer_file_path_name",
                                               task_ids="check_for_file_from_Retailer_to_organisation")
        destination_path = "/opt/airflow/Intermediat_Storage/"
        destination_file_name = "data.csv"
        destination_path_file_name = os.path.join(destination_path, destination_file_name)
        ti.xcom_push(key="destination_path_file_name", value=destination_path_file_name)
        shutil.move(Retailer_file_path_name, destination_path_file_name)
        return "File moved to Intermediat Storage"

    @task
    def transform_data(**kwargs):
        import pandas as pd
        import os
        destination_path_file_name = kwargs['ti'].xcom_pull(key="destination_path_file_name",
                                                            task_ids="move_file_from_data_to_intermediate_storage")
        df = pd.read_csv(destination_path_file_name,encoding="latin1")

        def Tranformations(df):
            df["InvoiceNo"] = df["InvoiceNo"].astype("int64", errors="ignore")
            df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])
            df["Country"] = df["Country"].astype("category")
            return df

        Tranformations(df)
        transformed_file_name = "data_transformed.csv"
        transformed_path_file_name = os.path.join("/opt/airflow/S3_Blob_Storage", transformed_file_name)
        df.to_csv("/opt/airflow/S3_Blob_Storage/data_transformed.csv", index=False)
        os.remove(destination_path_file_name)
        ti=kwargs['ti']
        ti.xcom_push(key="transformed_path_file_name", value=transformed_path_file_name)
        return "Data transformed and saved in S3_Blob_Storage"

    @task
    def load_data_into_warehouse_db(**kwargs):
        import pandas as pd
        from sqlalchemy import create_engine

            # Manually build the connection string
            # mysql is the service name from your docker-compose
        conn_string = "mysql+mysqldb://airflow:airflow@mysql:3306/usdb"
        engine = create_engine(conn_string)

        ti = kwargs['ti']
        transformed_path = ti.xcom_pull(key="transformed_path_file_name", task_ids="transform_data")

        df = pd.read_csv(transformed_path, encoding="latin1")
        df.to_sql("us_ecom_data", con=engine, if_exists="replace", index=False)
        return "Data loaded into Warehouse"

    @task
    def move_file_from_intermediate_to_backup(**kwargs):
        import os
        import shutil
        ti=kwargs['ti']
        transformed_path = ti.xcom_pull(key="transformed_path_file_name", task_ids="transform_data")
        destination_path = "/opt/airflow/Back_Up_Cold_Storage/"
        destination_file_name = "data_transformed.csv"
        destination_path_file_name = os.path.join(destination_path, destination_file_name)
        shutil.move(transformed_path, destination_path_file_name)
        return "File moved to Back_Up_Cold_Storage"


    check_for_file_from_Retailer_to_organisation=check_for_file_from_Retailer_to_organisation()
    move_file_from_data_to_intermediate_storage=move_file_from_data_to_intermediate_storage()
    transform_data=transform_data()
    load_data_into_warehouse_db=load_data_into_warehouse_db()
    move_file_from_intermediate_to_backup=move_file_from_intermediate_to_backup()

    check_for_file_from_Retailer_to_organisation >> move_file_from_data_to_intermediate_storage >> transform_data >> load_data_into_warehouse_db >> move_file_from_intermediate_to_backup

us_ecom_pipeline()




