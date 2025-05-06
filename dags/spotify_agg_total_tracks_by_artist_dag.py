from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from utils.common_utils import get_connection

def run_aggregate_total_tracks_by_artist():
    conn = get_connection()
    cur = conn.cursor()
    with open("/opt/airflow/scripts/aggregation/agg_total_tracks_by_artist.sql", "r") as f:
        sql = f.read()
        cur.execute(sql)
        conn.commit()
    cur.close()
    conn.close()
    print("✅ Aggregation: Total tracks by artist completed")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 13),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'email_on_failure': True,
    'email': ['your_email@example.com']
}

with DAG(
    dag_id="spotify_agg_total_tracks_by_artist",
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    wait_for_dw = ExternalTaskSensor(
        task_id="wait_for_transform_fact_track",
        external_dag_id="spotify_transform",
        external_task_id="transform_fact_track",
        mode="poke",
        timeout=600,
        poke_interval=30
    )

    aggregate_task = PythonOperator(
        task_id="aggregate_total_tracks_by_artist",
        python_callable=run_aggregate_total_tracks_by_artist
    )

    wait_for_dw >> aggregate_task
