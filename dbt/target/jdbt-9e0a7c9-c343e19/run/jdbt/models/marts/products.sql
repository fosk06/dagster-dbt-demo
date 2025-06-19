
  
    
    

    create  table
      "jaffle_platform"."main"."products__dbt_tmp"
  
    as (
      select * from "jaffle_platform"."main"."stg_products"
    );
  
  