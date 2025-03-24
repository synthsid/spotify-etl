import os
import json
import time
import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv()

# Spotify Authentication using .env file
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
            print(f"Rate limited. Retrying in {wait} seconds...")
            time.sleep(wait + 1)
        else:
            response.raise_for_status()
            return response.json()

# Data Load, Spotify API Calls
# Using Search endpoint to find artist we are looking for
# This way spotify handles the fuzzy matching and ranking
def search_artist(artist_name, token):
    url = "https://api.spotify.com/v1/search"
    params = {
        "q": artist_name,
        "type": "artist", # filters results to just artist, since we are doing a search
        "limit": 1
    }
    data = safe_request(url, auth_header(token), params)
    items = data.get("artists", {}).get("items", [])
    return items[0] if items else None

def get_top_tracks(artist_id, token):
    url = f"https://api.spotify.com/v1/artists/{artist_id}/top-tracks"
    params = {"country": "US"}
    data = safe_request(url, auth_header(token), params)
    return data.get("tracks", [])

# Insert our artist data into staging tables
# currently using on conflict do nothing to deal with duplicates
def insert_stg_artist(conn, artist):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO stg_artists (
                artist_id, name, followers, genres, popularity,
                external_url, image_url, raw_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (artist_id) DO NOTHING
        """, (
            artist["id"],
            artist["name"],
            artist["followers"]["total"],
            artist["genres"],
            artist["popularity"],
            artist["external_urls"].get("spotify"),
            artist["images"][0]["url"] if artist.get("images") else None,
            json.dumps(artist)
        ))
        conn.commit()

# Insert our tracks data into staging tables
# currently using on conflict do nothing to deal with duplicates
def insert_stg_track(conn, track, artist_id, artist_name):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO stg_tracks (
                track_id, name, artist_id, artist_name,
                album_name, release_date, duration_ms,
                popularity, explicit, external_url, raw_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (track_id) DO NOTHING
        """, (
            track["id"],
            track["name"],
            artist_id,
            artist_name,
            track["album"]["name"],
            track["album"]["release_date"],
            track["duration_ms"],
            track["popularity"],
            track["explicit"],
            track["external_urls"].get("spotify"),
            json.dumps(track)
        ))
        conn.commit()

# Loop over artist names -> then search for the artist -> insert artist data -> get top tracks -> insert tracks
def main():
    artist_names = [
        "Drake", "Taylor Swift", "Kendrick Lamar", "Adele", "Bad Bunny",
        "Billie Eilish", "SZA", "Travis Scott", "Dua Lipa", "Post Malone"
    ]

    token = get_token()
    conn = get_connection()

    for name in artist_names:
        print(f"Searching: {name}")
        artist = search_artist(name, token) # store in artist dictionary
        if not artist:
            print(f"Artist not found: {name}")
            continue

        insert_stg_artist(conn, artist)

        print(f"Fetching top tracks for {artist['name']}")
        tracks = get_top_tracks(artist["id"], token) # use artist id from the dictionary to get top tracks
        for track in tracks:
            insert_stg_track(conn, track, artist["id"], artist["name"])

    conn.close()
    print("ETL Complete!")

if __name__ == "__main__":
    main()
