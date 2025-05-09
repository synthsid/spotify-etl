-- agg_artist_popularity_trend.sql
-- Average daily popularity of an artist using the latest dim_artist mapping

CREATE TABLE IF NOT EXISTS agg_artist_popularity_trend AS
SELECT
    da.artist_key,
    da.artist_name,
    fap.snapshot_date,
    AVG(fap.popularity) AS avg_popularity
FROM fact_artist_popularity fap
JOIN dim_artist da
  ON fap.artist_id = da.artist_id
WHERE da.is_current = TRUE
GROUP BY da.artist_key, da.artist_name, fap.snapshot_date
ORDER BY da.artist_key, fap.snapshot_date;
