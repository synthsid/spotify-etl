-- Step 1: Insert new or updated albums (SCD2)
INSERT INTO dim_album (
    album_id, name, album_type, artist_id, total_tracks,
    release_date, external_url, is_current, start_date
)
SELECT
    s.raw_json->>'id' AS album_id,
    s.raw_json->>'name' AS name,
    s.raw_json->>'album_type' AS album_type,
    s.raw_json->'artists'->0->>'id' AS artist_id,
    (s.raw_json->>'total_tracks')::INT,
    CASE
        WHEN s.raw_json->>'release_date' ~ '^\d{4}-\d{2}-\d{2}$'
        THEN (s.raw_json->>'release_date')::DATE
        ELSE NULL
    END AS release_date,
    s.raw_json->'external_urls'->>'spotify' AS external_url,
    TRUE,
    CURRENT_TIMESTAMP
FROM stg_albums s
LEFT JOIN dim_album d
  ON s.raw_json->>'id' = d.album_id AND d.is_current = TRUE
WHERE
    d.album_id IS NULL
    OR (
        s.raw_json->>'name' IS DISTINCT FROM d.name
        OR s.raw_json->>'album_type' IS DISTINCT FROM d.album_type
        OR s.raw_json->'artists'->0->>'id' IS DISTINCT FROM d.artist_id
        OR (s.raw_json->>'total_tracks')::INT IS DISTINCT FROM d.total_tracks
        OR (
            CASE
                WHEN s.raw_json->>'release_date' ~ '^\d{4}-\d{2}-\d{2}$'
                THEN (s.raw_json->>'release_date')::DATE
                ELSE NULL
            END
            IS DISTINCT FROM d.release_date
        )
    );

-- Step 2: Expire old versions
UPDATE dim_album d
SET
    is_current = FALSE,
    end_date = CURRENT_TIMESTAMP
FROM stg_albums s
WHERE
    d.album_id = s.raw_json->>'id'
    AND d.is_current = TRUE
    AND (
        s.raw_json->>'name' IS DISTINCT FROM d.name
        OR s.raw_json->>'album_type' IS DISTINCT FROM d.album_type
        OR s.raw_json->'artists'->0->>'id' IS DISTINCT FROM d.artist_id
        OR (s.raw_json->>'total_tracks')::INT IS DISTINCT FROM d.total_tracks
        OR (
            CASE
                WHEN s.raw_json->>'release_date' ~ '^\d{4}-\d{2}-\d{2}$'
                THEN (s.raw_json->>'release_date')::DATE
                ELSE NULL
            END
            IS DISTINCT FROM d.release_date
        )
    );
