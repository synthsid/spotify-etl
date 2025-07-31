-- models/staging/stg_tracks.sql

with raw as (
    select * from public.tracks
)

select
    track_id,
    name as track_name,
    album_id,
    artist_id,
    duration_ms,
    explicit,
    popularity,
    external_url,
    preview_url,
    is_local,
    available_markets,
    updated_at
from raw
