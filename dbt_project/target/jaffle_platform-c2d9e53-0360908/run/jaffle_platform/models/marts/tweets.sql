
  
    
    

    create  table
      "jaffle_platform"."main"."tweets__dbt_tmp"
  
    as (
      select * from "jaffle_platform"."main"."stg_tweets"
    );
  
  