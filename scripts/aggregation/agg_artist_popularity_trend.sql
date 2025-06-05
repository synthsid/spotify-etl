CREATE TABLE IF NOT EXISTS agg_artist_popularity_trend (
    artist_key INT,
    artist_name TEXT,
    snapshot_date DATE,
    avg_popularity FLOAT,
    PRIMARY KEY (artist_key, snapshot_date)
);

TRUNCATE TABLE agg_artist_popularity_trend;

INSERT INTO agg_artist_popularity_trend (
    artist_key, artist_name, snapshot_date, avg_popularity
)
SELECT
    da.artist_key,
    da.name AS artist_name,
    ap.snapshot_date,
    AVG(ap.popularity) AS avg_popularity
FROM fact_artist_popularity ap
JOIN dim_artist da
  ON ap.artist_key = da.artist_key
WHERE da.is_current = TRUE
GROUP BY da.artist_key, da.name, ap.snapshot_date
ORDER BY da.artist_key, ap.snapshot_date;
