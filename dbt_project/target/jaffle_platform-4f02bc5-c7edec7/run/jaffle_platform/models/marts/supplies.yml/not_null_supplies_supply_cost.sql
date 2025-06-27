
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select supply_cost
from "jaffle_platform"."main"."supplies"
where supply_cost is null



  
  
      
    ) dbt_internal_test