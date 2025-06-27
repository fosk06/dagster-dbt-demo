
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select is_food_item
from "jaffle_platform"."main"."products"
where is_food_item is null



  
  
      
    ) dbt_internal_test