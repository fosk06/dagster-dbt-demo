
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select store_name
from "jaffle_platform"."main"."stg_stores"
where store_name is null



  
  
      
    ) dbt_internal_test