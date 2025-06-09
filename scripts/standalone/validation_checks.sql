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