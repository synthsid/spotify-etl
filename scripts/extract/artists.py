import json
import logging 
from utils.common_utils import get_token, get_connection, safe_request, auth_header

# Extract artist object from name
def search_artist(artist_name, token):
    url = "https://api.spotify.com/v1/search"
    params = {"q": artist_name, "type": "artist", "limit": 1}
    data = safe_request(url, auth_header(token), params)
    items = data.get("artists", {}).get("items",[])
    return items[0] if items else None

# Insert our artist data into staging tables
# currently using on conflict do nothing to deal with duplicates
def insert_stg_artist(conn, artist):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO stg_artists (artist_id, name, followers, genres, popularity, external_url, image_url, raw_json)
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

# ETL logic for artist only
def load_artists(conn, token, artist_names):
    artist_ids = []

    for name in artist_names:
        try:
            logging.info(f"Searching artist: {name}")
            artist = search_artist(name, token)
            if not artist:
                logging.warning(f"No artist found: {name}")
                continue

            insert_stg_artist(conn, artist)
            artist_ids.append(artist["id"])
        except Exception as e:
            logging.error(f"Error processing {name}: {e}")

    return artist_ids