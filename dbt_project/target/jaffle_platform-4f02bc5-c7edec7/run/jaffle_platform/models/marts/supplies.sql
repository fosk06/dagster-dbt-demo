
  
    
    

    create  table
      "jaffle_platform"."main"."supplies__dbt_tmp"
  
    as (
      select * from "jaffle_platform"."main"."stg_supplies"
    );
  
  