MODEL (
  name sqlmesh_jaffle_platform.supplies,
  kind FULL,
  tags ["dagster:group_name:sqlmesh_datamarts"],
  cron '@daily',
  grain (id, sku),
);

select * from sqlmesh_jaffle_platform.stg_supplies 