MODEL (
  name sqlmesh_jaffle_platform.stg_order_items,
  kind FULL,
  cron '*/5 * * * *',
  grain order_item_id,
  tags ["dagster:group_name:staging_sqlmesh"],
  audits(
    number_of_rows(threshold := 10),
    not_null(columns := (order_item_id, order_id, product_id))
  )
);

with source as (

    select * from main.raw_source_items

),

renamed as (

    select
        id as order_item_id,
        order_id,
        sku as product_id

    from source

)

select * from renamed 