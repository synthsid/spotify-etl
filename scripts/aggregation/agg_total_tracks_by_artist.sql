-- Agg_total_tracks_by_artist
-- Total number of tracks per artist

CREATE TABLE IF NOT EXISTS agg_total_tracks_by_artist (
    artist_key INT PRIMARY KEY,
    artist_name TEXT,
    total_tracks INT
);

TRUNCATE TABLE agg_total_tracks_by_artist;

INSERT INTO agg_total_tracks_by_artist (
    artist_key, artist_name, total_tracks
)
SELECT
    da.artist_key,
    da.name AS artist_name,
    COUNT(ft.track_id) AS total_tracks
FROM fact_track ft
JOIN dim_album al ON ft.album_key = al.album_key
JOIN dim_artist da ON al.artist_id = da.artist_id
WHERE da.end_date IS NULL
  AND al.end_date IS NULL
GROUP BY da.artist_key, da.name;


-- ToDo: make a version that incrementally updates the table using INSERT INTO instead of recreating the table