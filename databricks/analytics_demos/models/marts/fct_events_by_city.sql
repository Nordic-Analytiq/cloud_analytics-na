-- models/marts/fct_events_by_city.sql
-- Incremental model with MERGE strategy (native to Databricks tables).
-- The ref() call below creates a dependency on the staging model.
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'city'
) }}

with events as (

    select * from {{ ref('stg_events') }}

),

aggregated as (

    select
        city,
        count(*)           as event_count,
        min(event_date)    as first_event,
        max(event_date)    as last_event
    from events

    {% if is_incremental() %}
    -- On incremental runs, only process rows newer than what's already loaded
    where event_date > (select coalesce(max(last_event), '1900-01-01') from {{ this }})
    {% endif %}

    group by city

)

select * from aggregated