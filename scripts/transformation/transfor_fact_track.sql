INSERT INTO fact_track (
    track_id, name, artist_id, artist_name, album_name,
    release_date, duration_ms, explicit, external_url, snapshot_date
)
SELECT
    track_id,
    name,
    artist_id,
    artist_name,
    album_name,
    release_date::DATE,
    duration_ms,
    explicit,
    external_url,
    CURRENT_DATE
FROM stg_tracks
ON CONFLICT (track_id) DO NOTHING;
