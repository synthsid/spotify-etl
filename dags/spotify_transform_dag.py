from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from utils.common_utils import get_connection

# Transformation tasks

def transform_dim_artist():
    conn = get_connection()
    cur = conn.cursor()
    with open("/opt/airflow/scripts/transformation/transform_dim_artist_scd2.sql", "r") as f:
        cur.execute(f.read())
    conn.commit()
    cur.close()
    conn.close()
    print("dim_artist transformation completed")

def transform_dim_album():
    conn = get_connection()
    cur = conn.cursor()
    with open("/opt/airflow/scripts/transformation/transform_dim_album_scd2.sql", "r") as f:
        cur.execute(f.read())
    conn.commit()
    cur.close()
    conn.close()
    print("dim_album transformation completed")

def transform_fact_track():
    conn = get_connection()
    cur = conn.cursor()
    with open("/opt/airflow/scripts/transformation/transform_fact_track.sql", "r") as f:
        cur.execute(f.read())
    conn.commit()
    cur.close()
    conn.close()
    print("fact_track transformation completed")



# DAG Definition

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 13),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id="spotify_transform",
    default_args=default_args,
    schedule_interval='@daily',  # or '@hourly' if your ETL runs hourly
    catchup=False
)

# ExternalTaskSensor

wait_for_spotify_etl = ExternalTaskSensor(
    task_id="wait_for_spotify_etl",
    external_dag_id="spotify_etl",       # the dag_id of your extract DAG
    external_task_id="run_spotify_etl",   # the task_id inside that DAG
    mode="poke",
    timeout=600,
    poke_interval=60,
    dag=dag
)

# Transformation tasks

transform_artist = PythonOperator(
    task_id="transform_dim_artist",
    python_callable=transform_dim_artist,
    dag=dag
)

transform_album = PythonOperator(
    task_id="transform_dim_album",
    python_callable=transform_dim_album,
    dag=dag
)

transform_track = PythonOperator(
    task_id="transform_fact_track",
    python_callable=transform_fact_track,
    dag=dag
)

#Set Task Dependencies

wait_for_spotify_etl >> [transform_artist, transform_album] >> transform_track