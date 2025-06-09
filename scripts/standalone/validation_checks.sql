-- 1. Check for NULL artist_ids in dim_artist
SELECT * FROM dim_artist
WHERE artist_id IS NULL;

-- 2. Check for duplicate artist_ids (should be unique per is_current)
SELECT artist_id, COUNT(*) 
FROM dim_artist
WHERE is_current = TRUE
GROUP BY artist_id
HAVING COUNT(*) > 1;

-- 3. Check for NULL album_id or release_date
SELECT * FROM dim_album
WHERE album_id IS NULL OR release_date IS NULL;

-- 4. Check for future-dated albums
SELECT * FROM dim_album
WHERE release_date > CURRENT_DATE;

-- 5. Check for duplicate album_id rows
SELECT album_id, COUNT(*)
FROM dim_album
GROUP BY album_id
HAVING COUNT(*) > 1;

-- 6. Check for NULLs or negative values in fact_track
SELECT * FROM fact_track
WHERE track_id IS NULL
   OR artist_key IS NULL
   OR album_key IS NULL
   OR duration_ms IS NULL
   OR duration_ms < 0;

-- 7. Check for release_date values that couldn't be parsed
-- These would show up as release_date = '1970-01-01' (if defaulted badly)
SELECT * FROM fact_track
WHERE release_date < '1900-01-01';

-- 8. Check for duplicate track_id
SELECT track_id, COUNT(*)
FROM fact_track
GROUP BY track_id
HAVING COUNT(*) > 1;