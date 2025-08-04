MODEL (
  name sqlmesh_jaffle_platform.stg_stores,
  kind FULL,
  cron '*/5 * * * *',
  grain store_id,
  tags ["dagster:group_name:staging_sqlmesh"],
  audits(
    number_of_rows(threshold := 5),
    not_null(columns := (store_id))
  )
);


with source as (

    select * from main.raw_source_stores

),

renamed as (

    select
        ----------  ids
        id as store_id,

        ----------  strings
        name as store_name,

        ----------  timestamps
        opened_at as opened_at,

        ----------  numerics
        tax_rate

    from source

)

select * from renamed 