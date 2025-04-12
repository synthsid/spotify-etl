-- Step 1: Insert new artists or changed fields
INSERT INTO dim_artist (
    artist_id, name, genres, image_url, followers, is_current, start_date
)
SELECT
    s.raw_json->>'id' AS artist_id,
    s.raw_json->>'name' AS name,
    ARRAY(SELECT jsonb_array_elements_text(s.raw_json->'genres')) AS genres,
    s.raw_json->'images'->0->>'url' AS image_url,
    (s.raw_json->'followers'->>'total')::INT AS followers,
    TRUE AS is_current,
    CURRENT_TIMESTAMP AS start_date
FROM stg_artists s
LEFT JOIN dim_artist d
    ON s.raw_json->>'id' = d.artist_id AND d.is_current = TRUE
WHERE
    d.artist_id IS NULL
    OR (
        ARRAY(SELECT jsonb_array_elements_text(s.raw_json->'genres')) IS DISTINCT FROM d.genres
        OR s.raw_json->'images'->0->>'url' IS DISTINCT FROM d.image_url
    ); -- For now lets track genres and image

-- Step 2: Expire previous versions
UPDATE dim_artist d
SET
    is_current = FALSE,
    end_date = CURRENT_TIMESTAMP
FROM stg_artists s
WHERE
    d.artist_id = s.raw_json->>'id'
    AND d.is_current = TRUE
    AND (
        ARRAY(SELECT jsonb_array_elements_text(s.raw_json->'genres')) IS DISTINCT FROM d.genres
        OR s.raw_json->'images'->0->>'url' IS DISTINCT FROM d.image_url
    );

-- Step 3: Type 1 update for followers only
UPDATE dim_artist d
SET followers = (s.raw_json->'followers'->>'total')::INT
FROM stg_artists s
WHERE
    d.artist_id = s.raw_json->>'id'
    AND d.is_current = TRUE
    AND (
        ARRAY(SELECT jsonb_array_elements_text(s.raw_json->'genres')) IS NOT DISTINCT FROM d.genres
        AND s.raw_json->'images'->0->>'url' IS NOT DISTINCT FROM d.image_url
        AND (s.raw_json->'followers'->>'total')::INT IS DISTINCT FROM d.followers
    );

-- Step 4: Insert popularity snapshot
INSERT INTO fact_artist_popularity (artist_id, snapshot_date, popularity)
SELECT
    s.raw_json->>'id' AS artist_id,
    CURRENT_DATE,
    (s.raw_json->>'popularity')::INT
FROM stg_artists s
ON CONFLICT (artist_id, snapshot_date) DO NOTHING;