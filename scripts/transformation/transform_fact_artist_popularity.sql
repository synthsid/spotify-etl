-- Step 1: Insert daily popularity snapshot (skip if already loaded)
INSERT INTO fact_artist_popularity (
    artist_id,
    snapshot_date,
    popularity
)
SELECT
    raw_json->>'id' AS artist_id,
    CURRENT_DATE AS snapshot_date,
    (raw_json->>'popularity')::INT AS popularity
FROM stg_artists
ON CONFLICT (artist_id, snapshot_date) DO NOTHING;
