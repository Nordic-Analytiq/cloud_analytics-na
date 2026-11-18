-- models/marts/fct_events_by_city.sql
-- Incremental model with MERGE strategy (native to Databricks tables).
-- The ref() call below creates a dependency on the staging model.


with events as (

    select * from `format_demo_catalog`.`format_demo_staging`.`stg_events`

),

aggregated as (

    select
        city,
        count(*)           as event_count,
        min(event_date)    as first_event,
        max(event_date)    as last_event
    from events

    
    -- On incremental runs, only process rows newer than what's already loaded
    where event_date > (select coalesce(max(last_event), '1900-01-01') from `format_demo_catalog`.`format_demo_marts`.`fct_events_by_city`)
    

    group by city

)

select * from aggregated