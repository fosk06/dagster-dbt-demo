
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        supply_id, product_id
    from "jaffle_platform"."main"."supplies"
    group by supply_id, product_id
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test