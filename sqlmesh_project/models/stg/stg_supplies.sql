MODEL (
  name sqlmesh_jaffle_platform.stg_supplies,
  kind FULL,
  cron '@daily',
  grain supply_id,
  tags ["dagster:group_name:staging_sqlmesh"]
);

with source as (

    select * from main.raw_source_supplies

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