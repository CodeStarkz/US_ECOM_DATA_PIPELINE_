from airflow.decorators import dag, task
from pendulum import datetime

@dag(
    dag_id="us_ecom_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    is_paused_upon_creation=False
)

def us_ecom_pipeline():

    @task



