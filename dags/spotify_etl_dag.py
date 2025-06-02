from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests
import psycopg2
import os
import json
import time, logging
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

# Logging basics
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Authentication for Spotify API
# All of the credentials is in .env
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

def get_token():
    url = 'https://accounts.spotify.com/api/token'
    headers = {
        "Authorization": "Basic " + base64_credentials(),
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {"grant_type": "client_credentials"}
    response = requests.post(url, headers=headers, data=data)
    response.raise_for_status()
    return response.json()["access_token"]

# Converts your client_id:client_secret into base64 for Spotify’s API auth
def base64_credentials():
    import base64
    creds = f"{CLIENT_ID}:{CLIENT_SECRET}"
    return base64.b64encode(creds.encode()).decode()

# Returns the proper Authorization header for all Spotify API requests
def auth_header(token):
    return {"Authorization": f"Bearer {token}"}

# Database Connection, connects to spotify_db using credentials from .env
def get_connection():
    return psycopg2.connect(
        dbname=os.getenv("SPOTIFY_DB"),
        user=os.getenv("SPOTIFY_DB_USER"),
        password=os.getenv("SPOTIFY_DB_PASS"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )

# Rate-Limit-Safe API Request Wrapper
# If you hit a 429 response, it reads Retry-After and waits before retrying
def safe_request(url, headers, params=None):
    while True:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 429:
            wait = int(response.headers.get("Retry-After", 1))
            logging.warning(f"Rate limited. Retrying in {wait} seconds...")
            time.sleep(wait + 1)
        else:
            response.raise_for_status()
            return response.json()

# Data Load, Spotify API Calls
# Using Search endpoint to find artist we are looking for
# This way spotify handles the fuzzy matching and ranking
def search_artist(artist_name, token):
    url = "https://api.spotify.com/v1/search"
    params = {"q": artist_name, "type": "artist", "limit": 1} # filters results to just artist, since we are doing a search
    data = safe_request(url, auth_header(token), params)
    items = data.get("artists", {}).get("items", [])
    return items[0] if items else None

def get_artist_albums(artist_id, token):
    all_albums = []
    url = f"https://api.spotify.com/v1/artists/{artist_id}/albums"
    params = {
        "include_groups": "album,single",
        "limit": 50,
        "offset": 0
    }

    while url:
        data = safe_request(url, auth_header(token), params)
        items = data.get("items", [])
        all_albums.extend(items)

        url = data.get("next")
        params = None  # URL already includes params for the next page

    return all_albums

def get_album_tracks(album_id, token):
    url = f"https://api.spotify.com/v1/albums/{album_id}/tracks"
    data = safe_request(url, auth_header(token))
    return data.get("items", [])

def get_album(album_id, token):
    url = f"https://api.spotify.com/v1/albums/{album_id}"
    return safe_request(url, auth_header(token))

# Insert our artist data into staging tables
# currently using on conflict do nothing to deal with duplicates
def insert_stg_artist(conn, artist):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO stg_artists (artist_id, name, followers, genres, popularity, external_url, image_url, raw_json)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (artist_id) DO NOTHING
        """, (
            artist["id"], artist["name"], artist["followers"]["total"], artist["genres"], artist["popularity"],
            artist["external_urls"].get("spotify"),
            artist["images"][0]["url"] if artist.get("images") else None,
            json.dumps(artist)
        ))
        conn.commit()

def insert_stg_album(conn, album, artist_id):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO stg_albums (album_id, name, artist_id, release_date, total_tracks, album_type, external_url, raw_json)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (album_id) DO NOTHING
        """, (
            album["id"], album["name"], artist_id, album["release_date"], album["total_tracks"],
            album["album_type"], album["external_urls"].get("spotify"), json.dumps(album)
        ))
        conn.commit()

def insert_stg_track(conn, track, artist_id, artist_name, album_name, release_date):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO stg_tracks (
                track_id, name, artist_id, artist_name, album_name,
                release_date, duration_ms, popularity, explicit, external_url, raw_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, NULL, %s, %s, %s)
            ON CONFLICT (track_id) DO NOTHING
        """, (
            track["id"],
            track["name"],
            artist_id,
            artist_name,
            album_name,
            release_date,
            track.get("duration_ms"),
            track.get("explicit"),
            track["external_urls"].get("spotify"),
            json.dumps(track)
        ))
        conn.commit()


# Main ETL Task
# Loop over artist names -> then search for the artist -> insert artist data -> uses artist_id to get albums ->
# -> inserts album data -> uses album_id to get tracks -> insert tracks data
def run_spotify_etl():
    artist_names = [
        "Drake", "Taylor Swift", "Kendrick Lamar", "Adele", "Bad Bunny",
        "Billie Eilish", "SZA", "Travis Scott", "Dua Lipa", "Post Malone"
    ]

    token = get_token()
    conn = get_connection()

    for name in artist_names:
        try:
            logging.info(f"Searching artist: {name}")
            artist = search_artist(name, token) # store in artist dictionary
            if not artist:
                logging.warning(f"Artist not found: {name}")
                continue

            insert_stg_artist(conn, artist)

            logging.info(f"Fetching albums for {artist['name']}")
            albums = get_artist_albums(artist["id"], token) # store in albums dictionary
            for album in albums:
                album_id = album['id']
                album_data = get_album(album_id, token)
                insert_stg_album(conn, album_data, artist["id"])

                logging.info(f"Fetching tracks for album: {album_data['name']}")
                
                tracks = get_album_tracks(album["id"], token) # use album_id from the dictionary to get tracks
                for track in tracks:
                    insert_stg_track(
                        conn,
                        track,
                        artist_id=artist["id"],
                        artist_name=artist["name"],
                        album_name=album_data["name"],
                        release_date=album_data["release_date"]
                    )

        except Exception as e:
            logging.error(f"Error processing {name}: {e}")

    conn.close()
    logging.info("Spotify ETL DAG completed.")

# Airflow DAG
# Currently set-up to run hourly 
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='spotify_etl',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False
)

with dag:
    run_etl = PythonOperator(
        task_id='run_spotify_etl',
        python_callable=run_spotify_etl
    )
