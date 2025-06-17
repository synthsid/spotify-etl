import logging
from utils.common_utils import get_connection
from airflow.exceptions import AirflowFailException

logger = logging.getLogger(__name__)

VALIDATION_QUERIES = {
    "Null artist_id in dim_artist": """
        SELECT * FROM dim_artist WHERE artist_id IS NULL
    """,
    "Duplicate artist_id (is_current)": """
        SELECT artist_id FROM dim_artist
        WHERE is_current = TRUE
        GROUP BY artist_id HAVING COUNT(*) > 1
    """,
    "Future release dates in dim_album": """
        SELECT * FROM dim_album WHERE release_date > CURRENT_DATE
    """,
    "Negative track durations": """
        SELECT * FROM fact_track WHERE duration_ms < 0
    """,
    "Missing artist FK in fact_track": """
        SELECT ft.track_id FROM fact_track ft
        LEFT JOIN dim_artist da ON ft.artist_key = da.artist_key
        WHERE da.artist_key IS NULL
    """
}