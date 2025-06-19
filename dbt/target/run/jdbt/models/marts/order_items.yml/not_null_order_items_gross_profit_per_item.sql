
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select gross_profit_per_item
from "jaffle_platform"."main"."order_items"
where gross_profit_per_item is null



  
  
      
    ) dbt_internal_test