-- agg_album_stats.sql
-- Number of tracks, total duration, and average duration per album

CREATE TABLE IF NOT EXISTS agg_album_stats AS
SELECT
    da.album_key,
    da.album_name,
    COUNT(ft.track_id) AS num_tracks,
    SUM(ft.duration_ms) AS total_duration_ms,
    ROUND(AVG(ft.duration_ms), 2) AS avg_duration_ms
FROM fact_track ft
JOIN dim_album da ON ft.album_key = da.album_key
WHERE da.end_date IS NULL
GROUP BY da.album_key, da.album_name;
