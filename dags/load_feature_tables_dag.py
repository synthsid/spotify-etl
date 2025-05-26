from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="load_feature_tables",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    description="Transform and load audio features, artist genres, and track markets",
) as dag:
    

    load_dim_audio_feature = PostgresOperator(
        task_id="load_dim_audio_feature",
        postgres_conn_id="spotify_pg",
        sql="/opt/airflow/sql/transform/dim_audio_feature.sql"
    )

    load_dim_genre = PostgresOperator(
        task_id="load_dim_genre",
        postgres_conn_id="spotify_pg",
        sql="/opt/airflow/sql/transform/dim_genre.sql"
    )

    load_fact_artist_genre = PostgresOperator(
        task_id="load_fact_artist_genre",
        postgres_conn_id="spotify_pg",
        sql="/opt/airflow/sql/transform/fact_artist_genre.sql"
    )

    load_fact_track_market = PostgresOperator(
        task_id="load_fact_track_market",
        postgres_conn_id="spotify_pg",
        sql="/opt/airflow/sql/transform/fact_track_market.sql"
    )

    load_dim_audio_feature >> load_dim_genre >> load_fact_artist_genre
    load_dim_audio_feature >> load_fact_track_market