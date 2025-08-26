MODEL (
  name sqlmesh_jaffle_platform.stores,
  kind FULL,
  tags ["dagster:group_name:sqlmesh_datamarts"],
  cron '*/5 * * * *',
  grain store_id,
);

select * from sqlmesh_jaffle_platform.stg_stores 
