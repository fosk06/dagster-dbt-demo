MODEL (
  name sqlmesh_jaffle_platform.supplies,
  kind FULL,
  tags ["dagster:group_name:datamarts_sqlmesh", "datamarts"],
  cron '*/5 * * * *',
  grain (id, sku),
);

select * from sqlmesh_jaffle_platform.stg_supplies 