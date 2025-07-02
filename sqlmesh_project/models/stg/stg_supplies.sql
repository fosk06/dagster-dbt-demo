MODEL (
  name sqlmesh_jaffle_platform.stg_supplies,
  kind FULL,
  cron '@daily',
  grain supply_id,
);

with source as (

    select * from sqlmesh_jaffle_platform.raw_supplies

),

renamed as (

    select
        ----------  ids
        id as supply_id,
        sku as product_id,

        ----------  strings
        name as supply_name,

        ----------  numerics
        cost as supply_cost,

        ----------  booleans
        perishable

    from source

)

select * from renamed 