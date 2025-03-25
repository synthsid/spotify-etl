
SELECT
    s.raw_json->>'id' AS artist_id,
    s.raw_json->>'name' AS name,
    ARRAY(SELECT jsonb_array_elements_text(s.raw_json->'genres')) AS genres,
    (s.raw_json->>'popularity')::INT,
    s.raw_json->'images'->0->>'url' AS image_url,
    (s.raw_json->'followers'->>'total')::INT AS followers,
    TRUE AS is_current,
    CURRENT_TIMESTAMP AS start_date
FROM stg_artists s
LEFT JOIN dim_artist d
    ON s.raw_json->>'id' = d.artist_id AND d.is_current = TRUE
WHERE
    d.artist_id IS NULL -- New artist
    OR (
        -- SCD2 tracked field changes, can track other columns but these 3 look good
        ARRAY(SELECT jsonb_array_elements_text(s.raw_json->'genres')) IS DISTINCT FROM d.genres
        OR (s.raw_json->>'popularity')::INT IS DISTINCT FROM d.popularity
        OR s.raw_json->'images'->0->>'url' IS DISTINCT FROM d.image_url
    );