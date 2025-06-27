
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select tax_rate
from "jaffle_platform"."main"."stg_stores"
where tax_rate is null



  
  
      
    ) dbt_internal_test