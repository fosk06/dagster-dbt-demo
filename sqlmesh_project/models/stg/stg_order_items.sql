MODEL (
  name sqlmesh_jaffle_platform.stg_order_items,
  kind FULL,
  cron '@daily',
  grain order_item_id,
);

with source as (

    select * from sqlmesh_jaffle_platform.raw_items

),

renamed as (

    select
        id as order_item_id,
        order_id,
        sku as product_id

    from source

)

select * from renamed 