
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select opened_at
from "jaffle_platform"."main"."stg_stores"
where opened_at is null



  
  
      
    ) dbt_internal_test