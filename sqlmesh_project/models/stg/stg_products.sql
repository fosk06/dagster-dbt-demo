MODEL (
  name sqlmesh_jaffle_platform.stg_products,
  kind FULL,
  cron '*/5 * * * *',
  grain product_id,
  tags ["dagster:group_name:staging_sqlmesh"],
  audits(
    number_of_rows(threshold := 5),
    not_null(columns := (product_id, product_price))
  )
);


with source as (

    select * from main.raw_source_products

),

renamed as (

    select
        ----------  ids
        sku as product_id,

        ----------  strings
        name as product_name,
        type as product_type,
        description,

        ----------  numerics
        price as product_price,

        ----------  booleans
        type = 'food' as is_food_item,
        type = 'drink' as is_drink_item

    from source

)

select * from renamed 