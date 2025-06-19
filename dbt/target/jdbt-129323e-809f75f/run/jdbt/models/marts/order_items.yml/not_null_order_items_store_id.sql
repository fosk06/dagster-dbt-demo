
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select store_id
from "jaffle_platform"."main"."order_items"
where store_id is null



  
  
      
    ) dbt_internal_test