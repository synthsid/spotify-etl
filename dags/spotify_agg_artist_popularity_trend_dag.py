from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from utils.common_utils import get_connection

def run_agg_artist_popularity():
    conn = get_connection()
    cur = conn.cursor()
    with open("/opt/airflow/scripts/transformation/agg_artist_popularity_trend.sql", "r") as f:
        cur.execute(f.read())
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Aggregation: artist popularity trend completed")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 13),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id="spotify_agg_artist_popularity_trend",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:

    wait_for_transform_fact_track = ExternalTaskSensor(
        task_id="wait_for_transform_fact_track",
        external_dag_id="spotify_transform",
        external_task_id="transform_fact_track",
        mode="poke",
        timeout=600,
        poke_interval=30,
    )

    aggregate = PythonOperator(
        task_id="aggregate_artist_popularity_trend",
        python_callable=run_agg_artist_popularity,
    )

wait_for_transform_fact_track >> aggregate