
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select event_id
from `format_demo_catalog`.`format_demo_staging`.`stg_events`
where event_id is null



  
  
      
    ) dbt_internal_test