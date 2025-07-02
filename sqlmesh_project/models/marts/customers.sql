MODEL (
  name sqlmesh_jaffle_platform.customers,
  kind FULL,
  cron '@daily',
  grain customer_id,
);

with
customers as (
    select * from sqlmesh_jaffle_platform.stg_customers
),
orders as (
    select * from sqlmesh_jaffle_platform.stg_orders
),
customer_orders_summary as (
    select
        orders.customer_id,
        count(distinct orders.order_id) as count_lifetime_orders,
        count(distinct orders.order_id) > 1 as is_repeat_buyer,
        min(orders.order_date) as first_ordered_at,
        max(orders.order_date) as last_ordered_at,
        sum(orders.subtotal) as lifetime_spend_pretax,
        sum(orders.tax_paid) as lifetime_tax_paid,
        sum(orders.order_total) as lifetime_spend
    from orders
    group by 1
),
joined as (
    select
        customers.*,
        customer_orders_summary.count_lifetime_orders,
        customer_orders_summary.first_ordered_at,
        customer_orders_summary.last_ordered_at,
        customer_orders_summary.lifetime_spend_pretax,
        customer_orders_summary.lifetime_tax_paid,
        customer_orders_summary.lifetime_spend,
        case
            when customer_orders_summary.is_repeat_buyer then 'returning'
            else 'new'
        end as customer_type
    from customers
    left join customer_orders_summary
        on customers.customer_id = customer_orders_summary.customer_id
)
select * from joined 