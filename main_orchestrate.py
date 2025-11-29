from datetime import datetime, timedelta
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
import sys

sys.path.insert(0, "/home/hayden_huynh/Projects/Used-Car-Market-Analysis")
from main_extract import run_extract_sync
from main_transform import transform
from main_load import load

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 10,
    "retry_delay": timedelta(seconds=5),
    "email_on_failure": False,
    "email_on_retry": False,
    "priority_weight": 1000000,
}

with DAG(
    "cargurus_used_cars",
    default_args=default_args,
    description="ETL pipeline for used cars data from CarGurus",
    schedule="0 */2 * * *",  # At minute 0 every 2 hour
    start_date=datetime(2025, 11, 1, 0, 0),
    catchup=False,
    tags=["ETL", "CarGurus", "Used Cars"],
) as dag:

    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=run_extract_sync,
    )

    transform_data = PythonOperator(
        task_id="transform_data",
        python_callable=transform,
    )

    load_data = PythonOperator(
        task_id="load_data",
        python_callable=load,
    )

    extract_data >> transform_data >> load_data
