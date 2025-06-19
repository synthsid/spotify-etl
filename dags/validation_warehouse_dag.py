from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from validate_warehouse import validate_warehouse  # ← your function

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'email': ['you@example.com'],
    'email_on_failure': True
}

with DAG(
    dag_id="spotify_validate_warehouse",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="Validate Spotify data warehouse and log audit results"
) as dag:

    wait_for_transform = ExternalTaskSensor(
        task_id="wait_for_transform_fact_track",
        external_dag_id="spotify_transform",
        external_task_id="transform_fact_track",
        mode="poke",
        timeout=600,
        poke_interval=30
    )

    run_validation = PythonOperator(
        task_id="validate_warehouse_integrity",
        python_callable=validate_warehouse
    )

    wait_for_transform >> run_validation
