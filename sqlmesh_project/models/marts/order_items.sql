MODEL (
  name sqlmesh_jaffle_platform.order_items,
  kind FULL,
  cron '@daily',
  grain id,
);

with order_items as (
    select * from sqlmesh_jaffle_platform.stg_order_items
),
orders as (
    select * from sqlmesh_jaffle_platform.stg_orders
),
products as (
    select * from sqlmesh_jaffle_platform.stg_products
),
supplies as (
    select * from sqlmesh_jaffle_platform.stg_supplies
),
order_supplies_summary as (
    select
        product_id,
        sum(supply_cost) as supply_cost
    from supplies
    group by 1
),
joined as (
    select
        order_items.*,
        orders.order_date as ordered_at,
        products.product_name,
        products.product_price,
        products.is_food_item,
        products.is_drink_item,
        order_supplies_summary.supply_cost
    from order_items
    left join orders on order_items.order_id = orders.order_id
    left join products on order_items.product_id = products.product_id
    left join order_supplies_summary on order_items.product_id = order_supplies_summary.product_id
)

select * from joined 