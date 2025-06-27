
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select tweeted_at
from "jaffle_platform"."main"."tweets"
where tweeted_at is null



  
  
      
    ) dbt_internal_test