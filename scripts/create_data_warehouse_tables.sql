-- Using is_current with start and end date because its a type 2 dimensional table
-- artist_key will be our surrogate key for warehouse querrying
CREATE TABLE IF NOT EXISTS dim_artist (
    artist_key SERIAL PRIMARY KEY,
    artist_id TEXT NOT NULL,
    name TEXT,
    genres TEXT[],
    popularity INTEGER,
    image_url TEXT,
    followers INTEGER,
    is_current BOOLEAN DEFAULT TRUE,
    start_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_date TIMESTAMP
);
