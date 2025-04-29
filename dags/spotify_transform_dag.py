from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from utils.common_utils import get_connection

# === Transformation tasks ===

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