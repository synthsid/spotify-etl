-- agg_artist_popularity_trend.sql
-- Average daily popularity of an artist using the latest dim_artist mapping

CREATE TABLE IF NOT EXISTS agg_artist_popularity_trend AS
SELECT
    da.artist_key,
    da.name as artist_name,
    ap.snapshot_date,
    AVG(ap.popularity) AS avg_popularity
FROM fact_artist_popularity ap
JOIN dim_artist da
  ON ap.artist_key = da.artist_key
WHERE da.is_current = TRUE
GROUP BY da.artist_key, da.name, ap.snapshot_date
ORDER BY da.artist_key, ap.snapshot_date;
