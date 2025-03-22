from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.append('/opt/airflow/scripts')
from main import get_token, search_for_artist, get_songs_by_artist

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def run_etl():
    token = get_token()
    artist = search_for_artist(token, "Markoox")
    songs = get_songs_by_artist(token, artist["id"])
    for idx, song in enumerate(songs):
        print(f"{idx + 1}. {song['name']}")

with DAG(
    dag_id='spotify_etl_dag',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    etl_task = PythonOperator(
        task_id='spotify_etl',
        python_callable=run_etl
    )
