
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select sku
from "jaffle_platform"."main"."raw_products"
where sku is null



  
  
      
    ) dbt_internal_test