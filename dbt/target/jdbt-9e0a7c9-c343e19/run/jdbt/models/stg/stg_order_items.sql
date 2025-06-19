
  
  create view "jaffle_platform"."main"."stg_order_items__dbt_tmp" as (
    with source as (

    select * from "jaffle_platform"."main"."raw_items"

),

renamed as (

    select
        ----------  ids
        id as order_item_id,
        order_id,
        sku as product_id

    from source

)

select * from renamed
  );
