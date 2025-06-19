
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select perishable
from "jaffle_platform"."main"."stg_supplies"
where perishable is null



  
  
      
    ) dbt_internal_test