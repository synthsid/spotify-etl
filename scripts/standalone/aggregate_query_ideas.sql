

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
