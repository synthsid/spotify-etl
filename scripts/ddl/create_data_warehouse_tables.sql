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


CREATE TABLE IF NOT EXISTS dim_album (
    album_key SERIAL PRIMARY KEY,
    album_id TEXT NOT NULL,
    name TEXT,
    album_type TEXT,
    artist_id TEXT,
    total_tracks INTEGER,
    release_date DATE,
    external_url TEXT,
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


CREATE TABLE IF NOT EXISTS fact_track (
    track_id TEXT PRIMARY KEY,
    artist_key INTEGER,
    album_key INTEGER,
    name TEXT,
    release_date DATE,
    duration_ms INTEGER,
    explicit BOOLEAN,
    external_url TEXT,
    snapshot_date DATE DEFAULT CURRENT_DATE,
    FOREIGN KEY (artist_key) REFERENCES dim_artist(artist_key),
    FOREIGN KEY (album_key) REFERENCES dim_album(album_key)
);



-- dim_genre.sql
-- list of unique genres
CREATE TABLE IF NOT EXISTS dim_genre (
    genre_id SERIAL PRIMARY KEY,
    genre_name TEXT UNIQUE NOT NULL
);


--dim_audio_feature
-- audio fingerprint
CREATE TABLE IF NOT EXISTS dim_audio_feature (
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
    effective_date TIMESTAMP
);
