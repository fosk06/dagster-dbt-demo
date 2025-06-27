
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select supply_id
from "jaffle_platform"."main"."supplies"
where supply_id is null



  
  
      
    ) dbt_internal_test