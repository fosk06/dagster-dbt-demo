
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select is_perishable
from "jaffle_platform"."main"."supplies"
where is_perishable is null



  
  
      
    ) dbt_internal_test