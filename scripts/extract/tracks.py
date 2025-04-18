import json
import logging
from utils.common_utils import safe_request, auth_header

# Get all tracks for a given album
def get_album_tracks(album_id, token):
    url = f"https://api.spotify.com/v1/albums/{album_id}/tracks"
    data = safe_request(url, auth_header(token))
    return data.get("items", [])

# Insert track into stg_tracks
def insert_stg_track(conn, track, artist_id, artist_name, album_name, release_date):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO stg_tracks (
                track_id, name, artist_id, artist_name, album_name,
                release_date, duration_ms, popularity, explicit,
                external_url, raw_json
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, NULL, %s, %s, %s)
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

# Load all tracks for a list of albums
def load_tracks(conn, token, album_list):
    for album in album_list:
        try:
            album_id = album["id"]
            album_name = album["name"]
            release_date = album["release_date"]
            artist = album["artists"][0]
            artist_id = artist["id"]
            artist_name = artist["name"]

            logging.info(f"Fetching tracks for album: {album_name}")
            tracks = get_album_tracks(album_id, token)

            for track in tracks:
                insert_stg_track(conn, track, artist_id, artist_name, album_name, release_date)

        except Exception as e:
            logging.error(f"Error processing album '{album.get('name', '[unknown]')}': {e}")
