-- Agg_total_tracks_by_artist
-- Total number of tracks per artist

CREATE TABLE IF NOT EXISTS agg_total_tracks_by_artist AS
SELECT
    da.artist_key,
    da.name as artist_name,
    COUNT(ft.track_id) AS total_tracks
FROM fact_track ft
JOIN dim_album al ON ft.album_key = al.album_key
JOIN dim_artist da ON al.artist_id= da.artist_id
WHERE da.end_date IS NULL  -- Only include current artist records
  AND al.end_date IS NULL  -- Only include current album records
GROUP BY da.artist_key, da.name;



-- ToDo: make a version that incrementally updates the table using INSERT INTO instead of recreating the table