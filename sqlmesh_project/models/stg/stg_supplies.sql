MODEL (
  name sqlmesh_jaffle_platform.stg_supplies,
  kind FULL,
  cron '*/5 * * * *',
  grain supply_id,
  tags ["dagster:group_name:staging_sqlmesh"],
  audits(
    number_of_rows(threshold := 10),
    not_null(columns := (supply_id, product_id, supply_name, supply_cost, perishable)),
    not_constant(column := perishable)
  )
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