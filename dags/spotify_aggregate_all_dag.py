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

# can add email notification in default_args
# 'email': ['test@example.com'], 
# 'email_on_failure': True,
# 'email_on_retry': False  


def run_agg_total_tracks_by_artist():
    conn = get_connection()
    cur = conn.cursor()
    with open("/opt/airflow/scripts/aggregation/agg_total_tracks_by_artist.sql", "r") as f:
        cur.execute(f.read())
    conn.commit()
    cur.execute("SELECT COUNT(*) FROM agg_total_tracks_by_artist;")
    print("Total tracks by artist rows:", cur.fetchone()[0])
    cur.close()
    conn.close()

def run_agg_album_stats():
    conn = get_connection()
    cur = conn.cursor()
    with open("/opt/airflow/scripts/aggregation/agg_album_stats.sql", "r") as f:
        cur.execute(f.read())
    conn.commit()
    cur.execute("SELECT COUNT(*) FROM agg_album_stats;")
    print("Album stats rows:", cur.fetchone()[0])
    cur.close()
    conn.close()

def run_agg_artist_popularity():
    conn = get_connection()
    cur = conn.cursor()
    with open("/opt/airflow/scripts/aggregation/agg_artist_popularity_trend.sql", "r") as f:
        cur.execute(f.read())
    conn.commit()
    cur.execute("SELECT COUNT(*) FROM agg_artist_popularity_trend;")
    print("Artist popularity trend rows:", cur.fetchone()[0])
    cur.close()
    conn.close()

with DAG(
    dag_id="spotify_aggregate_all",
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

    agg_tracks_by_artist = PythonOperator(
        task_id="aggregate_total_tracks_by_artist",
        python_callable=run_agg_total_tracks_by_artist
    )

    agg_album_stats = PythonOperator(
        task_id="aggregate_album_stats",
        python_callable=run_agg_album_stats
    )

    agg_artist_popularity = PythonOperator(
        task_id="aggregate_artist_popularity_trend",
        python_callable=run_agg_artist_popularity
    )

# Running these aggregation tasks in parallel, we can make it chain instead
    wait_for_transform_fact_track >> [agg_tracks_by_artist, agg_album_stats, agg_artist_popularity]