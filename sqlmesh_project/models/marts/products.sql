MODEL (
  name sqlmesh_jaffle_platform.products,
  kind FULL,
  tags ["dagster:group_name:datamarts", "datamarts"],
  cron '@daily',
  grain sku,
);

select * from sqlmesh_jaffle_platform.stg_products 