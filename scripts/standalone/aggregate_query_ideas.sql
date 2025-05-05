

-- Agg_total_tracks_by_artist
-- Total number of tracks per artist

CREATE TABLE agg_total_tracks_by_artist AS
SELECT
    da.artist_key,
    da.artist_name,
    COUNT(ft.track_id) AS total_tracks
FROM fact_track ft
JOIN dim_album d_al ON ft.album_id = d_al.album_id
JOIN dim_artist da ON d_al.artist_id = da.artist_id
GROUP BY da.artist_key, da.artist_name;


-- album stats
-- Number of tracks, total duration, and average duration per album

CREATE TABLE agg_album_stats AS
SELECT
    da.album_id,
    da.album_name,
    COUNT(ft.track_id) AS num_tracks,
    SUM(ft.duration_ms) AS total_duration_ms,
    AVG(ft.duration_ms) AS avg_duration_ms
FROM fact_track ft
JOIN dim_album da ON ft.album_id = da.album_id
GROUP BY da.album_id, da.album_name;


-- artist popularity?
-- Popularity change of an artist over time
CREATE TABLE agg_artist_popularity_trend AS
SELECT
    artist_id,
    DATE(recorded_at) AS popularity_date,
    AVG(popularity) AS avg_popularity
FROM fact_artist_popularity
GROUP BY artist_id, DATE(recorded_at)
ORDER BY artist_id, popularity_date;

