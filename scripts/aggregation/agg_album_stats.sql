CREATE TABLE IF NOT EXISTS agg_album_stats (
    album_key INT PRIMARY KEY,
    album_name TEXT,
    num_tracks INT,
    total_duration_ms BIGINT,
    avg_duration_ms FLOAT
);

TRUNCATE TABLE agg_album_stats;

INSERT INTO agg_album_stats (
    album_key, album_name, num_tracks, total_duration_ms, avg_duration_ms
)
SELECT
    da.album_key,
    da.name AS album_name,
    COUNT(ft.track_id) AS num_tracks,
    SUM(ft.duration_ms) AS total_duration_ms,
    ROUND(AVG(ft.duration_ms), 2) AS avg_duration_ms
FROM fact_track ft
JOIN dim_album da ON ft.album_key = da.album_key
WHERE da.end_date IS NULL
GROUP BY da.album_key, da.name;
