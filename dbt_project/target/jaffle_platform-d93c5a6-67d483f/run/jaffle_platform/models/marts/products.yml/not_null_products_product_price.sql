
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select product_price
from "jaffle_platform"."main"."products"
where product_price is null



  
  
      
    ) dbt_internal_test