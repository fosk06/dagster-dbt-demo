
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select order_id
from "jaffle_platform"."main"."stg_order_items"
where order_id is null



  
  
      
    ) dbt_internal_test