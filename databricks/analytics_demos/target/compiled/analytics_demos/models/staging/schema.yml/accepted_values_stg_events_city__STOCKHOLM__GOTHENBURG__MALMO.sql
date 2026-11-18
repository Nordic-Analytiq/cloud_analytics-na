
    
    

with all_values as (

    select
        city as value_field,
        count(*) as n_records

    from `format_demo_catalog`.`format_demo_staging`.`stg_events`
    group by city

)

select *
from all_values
where value_field not in (
    'STOCKHOLM','GOTHENBURG','MALMO'
)


