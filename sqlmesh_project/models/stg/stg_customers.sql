MODEL (
  name sqlmesh_jaffle_platform.stg_customers,
  kind FULL,
  cron '*/5 * * * *',
  grain customer_id,
  tags ["dagster:group_name:staging_sqlmesh"],
  audits(
    number_of_rows(threshold := 10),
    not_null(columns := (customer_id))
  )
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