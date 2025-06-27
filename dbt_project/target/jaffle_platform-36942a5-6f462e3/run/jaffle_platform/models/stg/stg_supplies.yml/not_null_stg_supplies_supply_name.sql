
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select supply_name
from "jaffle_platform"."main"."stg_supplies"
where supply_name is null



  
  
      
    ) dbt_internal_test