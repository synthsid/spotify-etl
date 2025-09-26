/* This is just to keep some track of my sql queries I want to check within dbt*/
-- --   Usage: dbt run-operation run_sql --args '{"sql": ";"}' 

select count(*) from public.stg_tracks;
--   Usage: dbt run-operation run_sql --args '{"sql": "select count(*) from public.stg_tracks;"}' 

select * From public.fact_artist_popularity Order by snapshot_date DESC LIMIT 10; -- I need to check if my latest etl data ran properly. 
--   Usage: dbt run-operation run_sql --args '{"sql": "select * From public.fact_artist_popularity Order by snapshot_date DESC LIMIT 10;"}' 