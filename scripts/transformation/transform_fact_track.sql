-- Step 1: Insert cleaned tracks into fact_track
INSERT INTO fact_track (
    track_id, artist_key, album_key,
    name, release_date, duration_ms, explicit,
    external_url, snapshot_date
)
SELECT
    st.track_id,
    da.artist_key,
    al.album_key,
    st.name,
    st.release_date::DATE,
    st.duration_ms,
    st.explicit,
    st.external_url,
    CURRENT_DATE
FROM stg_tracks st
JOIN dim_artist da
  ON st.artist_id = da.artist_id
  AND da.is_current = TRUE
LEFT JOIN dim_album al
  ON st.album_name = al.name
 AND st.release_date::DATE = al.release_date
 AND al.is_current = TRUE
ON CONFLICT (track_id) DO NOTHING;
