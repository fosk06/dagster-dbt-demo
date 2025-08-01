MODEL (
  name sqlmesh_jaffle_platform.stg_customers,
  kind FULL,
  cron '@daily',
  grain customer_id,
  tags ["dagster:group_name:staging_sqlmesh"]
);

with source as (
    select * from main.raw_source_customers
),

renamed as (

    select
        id as customer_id,
        name as customer_name

    from source

)

select * from renamed