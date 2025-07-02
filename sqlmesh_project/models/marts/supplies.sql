MODEL (
  name sqlmesh_jaffle_platform.supplies,
  kind FULL,
  cron '@daily',
  grain (id, sku),
);

select * from sqlmesh_jaffle_platform.stg_supplies 