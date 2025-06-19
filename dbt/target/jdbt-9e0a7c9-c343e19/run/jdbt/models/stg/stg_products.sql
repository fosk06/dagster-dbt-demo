
  
  create view "jaffle_platform"."main"."stg_products__dbt_tmp" as (
    with source as (

    select * from "jaffle_platform"."main"."raw_products"

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
  );
