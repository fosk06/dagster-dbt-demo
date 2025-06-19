
  
  create view "jaffle_platform"."main"."stg_customers__dbt_tmp" as (
    with source as (
    select * from "jaffle_platform"."main"."raw_customers"

),

renamed as (

    select
        id as customer_id,
        name as customer_name

    from source

)

select * from renamed
  );
