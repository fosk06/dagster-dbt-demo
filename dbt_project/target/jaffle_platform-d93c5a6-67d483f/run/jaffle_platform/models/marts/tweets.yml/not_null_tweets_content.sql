
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select content
from "jaffle_platform"."main"."tweets"
where content is null



  
  
      
    ) dbt_internal_test