-- models/staging/stg_events.sql
-- Staging model: clean + rename the raw source.
-- The source() call below resolves to format_demo_catalog.format_demo.events_delta

with source as (

    select * from {{ source('format_demo', 'events_delta') }}

),

renamed as (

    select
        cast(id as int)             as event_id,
        cast(event_date as date)    as event_date,
        upper(city)                 as city
    from source

)

select * from renamed
where event_date >= '2026-11-01'