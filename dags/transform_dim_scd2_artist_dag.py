from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import psycopg2
from dotenv import load_dotenv
from common_utils import get_connection

load_dotenv()

def run_artist_transformation():
    conn = get_connection()
    cur = conn.cursor()

    # Load and run the transformation SQL
    with open("/opt/airflow/scripts/transformation/transform_dim_scd2_artist.sql", "r") as f:
        sql = f.read()
        cur.execute(sql)
        conn.commit()

    cur.close()
    conn.close()
    print("transform_dim_artist_scd2 completed")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 24),
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

dag = DAG(
    dag_id="spotify_transform_artist",
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

transform_task = PythonOperator(
    task_id="transform_dim_scd2_artist",
    python_callable=run_artist_transformation,
    dag=dag
)
