from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from utils.common_utils import get_connection

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 13),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

def run_agg_total_tracks_by_artist():
    conn = get_connection()
    cur = conn.cursor()
    with open("/opt/airflow/scripts/transformation/agg_total_tracks_by_artist.sql", "r") as f:
        cur.execute(f.read())
    conn.commit()
    cur.execute("SELECT COUNT(*) FROM agg_total_tracks_by_artist;")
    print("Total tracks by artist rows:", cur.fetchone()[0])
    cur.close()
    conn.close()

def run_agg_album_stats():
    conn = get_connection()
    cur = conn.cursor()
    with open("/opt/airflow/scripts/transformation/agg_album_stats.sql", "r") as f:
        cur.execute(f.read())
    conn.commit()
    cur.execute("SELECT COUNT(*) FROM agg_album_stats;")
    print("Album stats rows:", cur.fetchone()[0])
    cur.close()
    conn.close()

def run_agg_artist_popularity():
    conn = get_connection()
    cur = conn.cursor()
    with open("/opt/airflow/scripts/transformation/agg_artist_popularity_trend.sql", "r") as f:
        cur.execute(f.read())
    conn.commit()
    cur.execute("SELECT COUNT(*) FROM agg_artist_popularity_trend;")
    print("Artist popularity trend rows:", cur.fetchone()[0])
    cur.close()
    conn.close()