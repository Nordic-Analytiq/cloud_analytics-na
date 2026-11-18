-- back compat for old kwarg name
  
  
  
  
  
  
      
          
          
      
  

    merge
    into
        `format_demo_catalog`.`format_demo_marts`.`fct_events_by_city` as DBT_INTERNAL_DEST
    using
        `fct_events_by_city__dbt_tmp` as DBT_INTERNAL_SOURCE
    on
        
              DBT_INTERNAL_SOURCE.`city` <=> DBT_INTERNAL_DEST.`city`
          
    when matched
        then update set
            *
    when not matched
        then insert
            *
