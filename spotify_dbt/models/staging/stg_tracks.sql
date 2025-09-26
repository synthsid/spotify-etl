with raw as (
    select * from {{ source('raw', 'stg_tracks') }}
)

select *
from raw
