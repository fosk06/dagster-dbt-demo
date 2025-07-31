MODEL (
  name sqlmesh_jaffle_platform.orders,
  kind FULL,
  cron '*/5 * * * *',
  grain order_id,
  tags ["dagster:group_name:sqlmesh_datamarts"],
  partitioned_by = ["order_date"],
  audits(
    number_of_rows(threshold := 10),
    not_null(columns := (order_id, customer_id, store_id, order_date, order_total, status)),
    unique_values(columns := (order_id))
  ),

  column_descriptions (
    order_id = 'Unique identifier for each order',
    customer_id = 'Identifier linking to the customer who placed the order',
    order_date = 'Date when the order was placed',
    store_id = 'Identifier of the store where the order was placed',
    subtotal = 'Order subtotal before tax and discounts',
    tax_paid = 'Tax amount paid on the order',
    order_total = 'Total order amount including tax',
    status = 'Current status of the order (pending, completed, cancelled, etc.)',
    order_cost = 'Total cost of supplies/ingredients for the order',
    order_items_subtotal = 'Sum of all product prices in the order',
    count_food_items = 'Number of food items in the order',
    count_drink_items = 'Number of drink items in the order',
    count_order_items = 'Total number of items in the order',
    is_food_order = 'Boolean indicating if the order contains food items',
    is_drink_order = 'Boolean indicating if the order contains drink items',
    customer_order_number = 'Sequential order number for each customer (1st, 2nd, 3rd order, etc.)'
  )
);

with orders as (
    select * from sqlmesh_jaffle_platform.stg_orders
),
order_items as (
    select * from sqlmesh_jaffle_platform.order_items
),
order_items_summary as (
    select
        order_id,
        sum(supply_cost) as order_cost,
        sum(product_price) as order_items_subtotal,
        count(order_item_id) as count_order_items,
        sum(case when is_food_item then 1 else 0 end) as count_food_items,
        sum(case when is_drink_item then 1 else 0 end) as count_drink_items
    from order_items
    group by 1
),
compute_booleans as (
    select
        orders.*,
        order_items_summary.order_cost,
        order_items_summary.order_items_subtotal,
        order_items_summary.count_food_items,
        order_items_summary.count_drink_items,
        order_items_summary.count_order_items,
        order_items_summary.count_food_items > 0 as is_food_order,
        order_items_summary.count_drink_items > 0 as is_drink_order,
        true as is_truess
    from orders
    left join order_items_summary
        on orders.order_id = order_items_summary.order_id
),
customer_order_count as (
    select
        *,
        row_number() over (
            partition by customer_id
            order by order_date asc
        ) as customer_order_number
    from compute_booleans
)

select * from customer_order_count 