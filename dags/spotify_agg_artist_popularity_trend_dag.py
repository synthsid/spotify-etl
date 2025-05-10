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