MODEL (
  name sqlmesh_jaffle_platform.stg_customers,
  kind FULL,
  cron '@daily',
  grain customer_id,
);

with source as (
    select * from sqlmesh_jaffle_platform.raw_customers
),

renamed as (

    select
        id as customer_id,
        name as customer_name

    from source

)

select * from renamed