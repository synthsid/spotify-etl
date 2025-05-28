import json
import logging
from utils.common_utils import get_token, auth_header, safe_request, get_connection

# Set up logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def extract_artist_genres():
    conn = get_connection()
    cur = conn.cursor()

    token = get_token()
    headers = auth_header(token)

    cur.execute("SELECT artist_id FROM stg_artists")
    artist_ids = [row[0] for row in cur.fetchall()]
    logger.info(f"Fetched {len(artist_ids)} artist IDs from stg_artists")

    for artist_id in artist_ids:
        url = f"https://api.spotify.com/v1/artists/{artist_id}"
        data = safe_request(url, headers=headers)

        if data:
            genres = data.get("genres", [])
            for genre in genres:
                try:
                    cur.execute("""
                        INSERT INTO stg_artist_genres (artist_id, genre)
                        VALUES (%s, %s)
                        ON CONFLICT DO NOTHING;
                    """, (artist_id, genre))
                    logger.info(f"Inserted genre '{genre}' for artist: {artist_id}")
                except Exception as e:
                    logger.error(f"Failed to insert genre for artist {artist_id}: {e}")
                    logger.debug(f"Payload: {json.dumps(data, indent=2)}")

    conn.commit()
    cur.close()
    conn.close()
    logger.info("Finished loading artist genres into stg_artist_genres.")
