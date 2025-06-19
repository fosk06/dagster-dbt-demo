
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_profit
from "jaffle_platform"."main"."orders"
where total_profit is null



  
  
      
    ) dbt_internal_test