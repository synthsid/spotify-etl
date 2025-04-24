INSERT INTO fact_track (
    track_id, artist_key, album_key,
    name, release_date, duration_ms, explicit, external_url, snapshot_date
)
SELECT
    st.track_id,
    da.artist_key,
    NULL AS album_key,  -- Update this when dim_album is built
    st.name,
    st.release_date::DATE,
    st.duration_ms,
    st.explicit,
    st.external_url,
    CURRENT_DATE
FROM stg_tracks st
JOIN dim_artist da
  ON st.artist_id = da.artist_id AND da.is_current = TRUE
ON CONFLICT (track_id) DO NOTHING;
