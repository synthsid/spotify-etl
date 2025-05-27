import json
import logging
from utils.common_utils import get_token, auth_header, safe_request, get_connection

# Set up logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def extract_audio_features():
    conn = get_connection()
    cur = conn.cursor()

    token = get_token()
    headers = auth_header(token)

    cur.execute("SELECT track_id FROM stg_tracks")
    track_ids = [row[0] for row in cur.fetchall()]
    logger.info(f"Fetched {len(track_ids)} track IDs from stg_tracks")

    for track_id in track_ids:
        url = f"https://api.spotify.com/v1/audio-features/{track_id}"
        af = safe_request(url, headers=headers)

        if af:
            try:
                cur.execute("""
                    INSERT INTO stg_audio_features (
                        track_id, danceability, energy, key, loudness, mode,
                        speechiness, acousticness, instrumentalness, liveness, valence,
                        tempo, duration_ms, time_signature
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (track_id) DO NOTHING;
                """, (
                    af['id'], af['danceability'], af['energy'], af['key'], af['loudness'], af['mode'],
                    af['speechiness'], af['acousticness'], af['instrumentalness'], af['liveness'], af['valence'],
                    af['tempo'], af['duration_ms'], af['time_signature']
                ))
                logger.info(f"Inserted audio features for track: {track_id}")
            except Exception as e:
                logger.error(f"Failed to insert audio features for {track_id}: {e}")
                logger.debug(f"Payload: {json.dumps(af, indent=2)}")

    conn.commit()
    cur.close()
    conn.close()
    logger.info("Finished loading audio features into stg_audio_features.")
