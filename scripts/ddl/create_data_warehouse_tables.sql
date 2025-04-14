/*
Dimensional Tables
*/

-- Using is_current with start and end date because its a type 2 dimensional table
-- artist_key will be our surrogate key for warehouse querrying
CREATE TABLE IF NOT EXISTS dim_artist (
    artist_key SERIAL PRIMARY KEY,
    artist_id TEXT NOT NULL,
    name TEXT,
    genres TEXT[],
    image_url TEXT,
    followers INTEGER,
    is_current BOOLEAN DEFAULT TRUE,
    start_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_date TIMESTAMP
);


/*
Fact Tables
*/

CREATE TABLE IF NOT EXISTS fact_artist_popularity (
    artist_key INTEGER NOT NULL,  -- Surrogate key, not artist_id
    snapshot_date DATE DEFAULT CURRENT_DATE,
    popularity INTEGER,
    PRIMARY KEY (artist_key, snapshot_date),
    FOREIGN KEY (artist_key) REFERENCES dim_artist(artist_key)
);