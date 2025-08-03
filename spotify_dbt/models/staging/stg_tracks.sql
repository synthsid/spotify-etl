-- models/staging/stg_tracks.sql

with raw as (
    select * from public.stg_tracks
)

select
    *
from raw
