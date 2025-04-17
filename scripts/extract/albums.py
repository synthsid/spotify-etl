import json
import logging
from utils.common_utils import get_token, safe_request, auth_header


# Get all albums (albums + singles) for an artist
def load_albums(conn, token, artist_ids):
    all_albums = []
    url = f"https://api.spotify.com/v1/artists/{artist_id}/albums"
    params = {"include_groups": "album,single", "limit": 50, "offset": 0}

    while url:
        data = safe_request(url, auth_header(token), params)
        items = data.get("items", [])
        all_albums .extend(items)

        url = data.get("next")
        params = None # Don't reuse original params once Spotify gives us a full 'next' URL

    return all_albums

# Get album details using spotify api url 
def get_album(album_id, token):
    url = f"https://api.spotify.com/v1/albums/{album_id}"
    return safe_request(url, auth_header(token))

# Insert album records into stg_albums
def insert_stg_album(conn, album):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO stg_albums (
                album_id, name, artist_id, release_date,
                total_tracks, album_type, external_url, raw_json
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (album_id) DO NOTHING
        """, (
            album["id"],
            album["name"],
            album["artists"][0]["id"],
            album["release_date"],
            album["total_tracks"],
            album["album_type"],
            album["external_urls"]["spotify"],
            json.dumps(album)
        ))
        conn.commit()

# ETL Function
def load_albums(conn, token, artist_ids):
    album_ids = set()

    for artist_id in artist_ids:
        try:
            logging.info(f"Fetching albums for artist: {artist_id}")
            albums = get_artist_albums(artist_id, token)

            for album in albums:
                album_id = album["id"]
                if album_id in album_ids:
                    continue

                album_data = get_album(album_id, token)
                insert_stg_album(conn, album_data)
                album_ids.add(album_id)

        except Exception as e:
            logging.error(f"Error processing albums for artist {artist_id}: {e}")

    return list(album_ids)