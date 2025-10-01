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
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": False,
    "email_on_retry": False,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function, # or list of functions
    # 'on_success_callback': some_other_function, # or list of functions
    # 'on_retry_callback': another_function, # or list of functions
    # 'sla_miss_callback': yet_another_function, # or list of functions
    # 'on_skipped_callback': another_function, #or list of functions
    # 'trigger_rule': 'all_success'
}

with DAG(
    "cargurus_used_cars",
    default_args=default_args,
    description="ETL pipeline for used cars data from CarGurus",
    schedule="0 * * * *",  # At minute 0 every hour
    start_date=datetime(2025, 8, 27, 14, 0),
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
