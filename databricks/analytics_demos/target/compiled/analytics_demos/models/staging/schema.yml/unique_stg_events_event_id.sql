
    
    

select
    event_id as unique_field,
    count(*) as n_records

from `format_demo_catalog`.`format_demo_staging`.`stg_events`
where event_id is not null
group by event_id
having count(*) > 1


