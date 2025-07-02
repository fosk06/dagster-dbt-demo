MODEL (
  name sqlmesh_jaffle_platform.products,
  kind FULL,
  cron '@daily',
  grain sku,
);

select * from sqlmesh_jaffle_platform.stg_products 