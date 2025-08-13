MODEL (
  name sqlmesh_jaffle_platform.full_model,
  kind FULL,
  cron '@daily',
  grain item_id,
  audits (assert_positive_order_ids),
  tags ["dagster:group_name:staging"]
);

SELECT
  item_id,
  COUNT(DISTINCT id) AS num_orders,
FROM
  sqlmesh_jaffle_platform.incremental_model
GROUP BY item_id
  