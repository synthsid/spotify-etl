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

CREATE TABLE IF NOT EXISTS stg_albums (
    album_id TEXT PRIMARY KEY,
    name TEXT,
    artist_id TEXT,
    release_date TEXT,
    total_tracks INTEGER,
    album_type TEXT,
    external_url TEXT,
    raw_json JSONB,
    pulled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Pull in audio features
-- this data will help with understanding of energy, tempo, and other features of a track
-- could help understand mood/genre etc

-- Genre needs to be pulled from artist metadata 
-- but could turn into dim-genre

-- could also dig into markets/geography 


-- stg_audio_features.sql – for storing raw audio features per track

CREATE TABLE IF NOT EXISTS stg_audio_features (
    track_id TEXT PRIMARY KEY,
    danceability FLOAT,
    energy FLOAT,
    key INTEGER,
    loudness FLOAT,
    mode INTEGER,
    speechiness FLOAT,
    acousticness FLOAT,
    instrumentalness FLOAT,
    liveness FLOAT,
    valence FLOAT,
    tempo FLOAT,
    duration_ms INTEGER,
    time_signature INTEGER,
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- stg_artist_genres.sql – for mapping artists to multiple genres

CREATE TABLE IF NOT EXISTS stg_artist_genres (
    artist_id TEXT,
    genre TEXT,
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (artist_id, genre)
);


-- stg_track_markets.sql – for tracking where each track is available

CREATE TABLE IF NOT EXISTS stg_track_markets (
    track_id TEXT,
    market_code TEXT,  -- ISO country code, e.g., 'US', 'GB'
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (track_id, market_code)
);

