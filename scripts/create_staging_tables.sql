-- Create staging table for raw artist data
CREATE TABLE IF NOT EXISTS stg_artists (
    artist_id TEXT PRIMARY KEY,
    name TEXT,
    followers INTEGER,
    genres TEXT[],
    popularity INTEGER,
    external_url TEXT,
    image_url TEXT,
    raw_json JSONB,
    pulled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create staging table for raw track data
CREATE TABLE IF NOT EXISTS stg_tracks (
    track_id TEXT PRIMARY KEY,
    name TEXT,
    artist_id TEXT,
    artist_name TEXT,
    album_name TEXT,
    release_date TEXT,
    duration_ms INTEGER,
    popularity INTEGER,
    explicit BOOLEAN,
    external_url TEXT,
    raw_json JSONB,
    pulled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
