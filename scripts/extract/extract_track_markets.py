import json
import logging
from utils.common_utils import get_token, auth_header, safe_request, get_connection

# Set up logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def extract_track_markets():
    conn = get_connection()
    cur = conn.cursor()

    token = get_token()
    headers = auth_header(token)

    cur.execute("SELECT track_id FROM stg_tracks")
    track_ids = [row[0] for row in cur.fetchall()]
    logger.info(f"Fetched {len(track_ids)} track IDs from stg_tracks")

    for track_id in track_ids:
        url = f"https://api.spotify.com/v1/tracks/{track_id}"
        data = safe_request(url, headers=headers)

        if data:
            markets = data.get("available_markets", [])
            for market_code in markets:
                try:
                    cur.execute("""
                        INSERT INTO stg_track_markets (track_id, market_code)
                        VALUES (%s, %s)
                        ON CONFLICT DO NOTHING;
                    """, (track_id, market_code))
                    logger.info(f"Inserted market '{market_code}' for track: {track_id}")
                except Exception as e:
                    logger.error(f"Failed to insert market for track {track_id}: {e}")
                    logger.debug(f"Payload: {json.dumps(data, indent=2)}")

    conn.commit()
    cur.close()
    conn.close()
    logger.info("Finished loading track markets into stg_track_markets.")
