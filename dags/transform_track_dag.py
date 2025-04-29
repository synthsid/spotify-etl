from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from utils.common_utils import get_connection

def transform_fact_track():
    conn = get_connection()
    cur = conn.cursor()
    with open("/opt/airflow/scripts/transformation/transform_fact_track.sql", "r") as f:
        sql = f.read()
        cur.execute(sql)
        conn.commit()
    cur.close()
    conn.close()
    print("fact_track transformation completed")

dag = DAG(
    dag_id="spotify_transform_fact_track",
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2025, 4, 13),
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    schedule_interval='@daily',
    catchup=False
)

transform_task = PythonOperator(
    task_id="transform_fact_track",
    python_callable=transform_fact_track,
    dag=dag
)
