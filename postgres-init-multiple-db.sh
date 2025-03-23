#!/bin/bash
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
  -- Create the Spotify database
  CREATE DATABASE $SPOTIFY_DB;

  -- Create the Spotify user with login and password
  CREATE ROLE $SPOTIFY_DB_USER WITH LOGIN PASSWORD '$SPOTIFY_DB_PASS';

  -- Give the user full privileges on the Spotify DB
  GRANT ALL PRIVILEGES ON DATABASE $SPOTIFY_DB TO $SPOTIFY_DB_USER;
EOSQL
