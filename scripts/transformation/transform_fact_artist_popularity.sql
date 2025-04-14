-- Step 1: Insert daily popularity snapshot (skip if already loaded)
INSERT INTO fact_artist_popularity (
    artist_key,
    snapshot_date,
    popularity
)
SELECT
    d.artist_key,
    CURRENT_DATE AS snapshot_date,
    (s.raw_json->>'popularity')::INT As popularity
FROM stg_artists s
JOIN dim_artist d  
ON   s.raw->>'id' = d.artist_id
WHERE d.is_current = TRUE
ON CONFLICT (artist_key, snapshot_date) DO NOTHING;