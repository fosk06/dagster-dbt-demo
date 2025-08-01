MODEL (
  name sqlmesh_jaffle_platform.customers,
  kind FULL,
  tags ["dagster:group_name:sqlmesh_datamarts"],
  cron '*/10 * * * *',
  grain customer_id
);

WITH customers AS (
  SELECT
    *
  FROM sqlmesh_jaffle_platform.stg_customers
), orders AS (
  SELECT
    *
  FROM sqlmesh_jaffle_platform.stg_orders
), customer_orders_summary AS (
  SELECT
    orders.customer_id,
    COUNT(DISTINCT orders.order_id) AS count_lifetime_orders,
    COUNT(DISTINCT orders.order_id) > 1 AS is_repeat_buyer,
    MIN(orders.order_date) AS first_ordered_at,
    MAX(orders.order_date) AS last_ordered_at,
    SUM(orders.subtotal) AS lifetime_spend_pretax,
    SUM(orders.tax_paid) AS lifetime_tax_paid,
    SUM(orders.order_total) AS lifetime_spend
  FROM orders
  GROUP BY
    1
), joined AS (
  SELECT
    customers.*,
    customer_orders_summary.count_lifetime_orders,
    customer_orders_summary.first_ordered_at,
    customer_orders_summary.last_ordered_at,
    TRUE AS is_true,
    customer_orders_summary.lifetime_spend_pretax,
    customer_orders_summary.lifetime_tax_paid,
    customer_orders_summary.lifetime_spend,
    CASE WHEN customer_orders_summary.is_repeat_buyer THEN 'returning' ELSE 'new' END AS customer_type
  FROM customers
  LEFT JOIN customer_orders_summary
    ON customers.customer_id = customer_orders_summary.customer_id
)
SELECT
  *
FROM joined