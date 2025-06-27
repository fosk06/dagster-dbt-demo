with orders as (

    select * from {{ ref('stg_orders') }}

),

order_items as (

    select * from {{ ref('order_items') }}

),

order_items_summary as (

    select
        order_id,
        
        sum(case when is_food_item then 1 else 0 end) as food_items_count,
        sum(case when is_drink_item then 1 else 0 end) as drink_items_count,
        count(*) as total_items_count,

        {{ cents_to_dollars('sum(case when is_food_item then product_price * 100 else 0 end)') }} as food_revenue,
        {{ cents_to_dollars('sum(case when is_drink_item then product_price * 100 else 0 end)') }} as drink_revenue,
        {{ cents_to_dollars('sum(product_price * 100)') }} as total_revenue,

        {{ cents_to_dollars('sum(case when is_food_item then supply_cost * 100 else 0 end)') }} as food_cost,
        {{ cents_to_dollars('sum(case when is_drink_item then supply_cost * 100 else 0 end)') }} as drink_cost,
        {{ cents_to_dollars('sum(supply_cost * 100)') }} as total_cost,

        {{ cents_to_dollars('sum(case when is_food_item then gross_profit_per_item * 100 else 0 end)') }} as food_profit,
        {{ cents_to_dollars('sum(case when is_drink_item then gross_profit_per_item * 100 else 0 end)') }} as drink_profit,
        {{ cents_to_dollars('sum(gross_profit_per_item * 100)') }} as total_profit

    from order_items
    group by 1

),

joined as (

    select
        orders.order_id,
        orders.customer_id,
        orders.store_id,
        orders.order_date,
        orders.status,

        order_items_summary.food_items_count,
        order_items_summary.drink_items_count,
        order_items_summary.total_items_count,

        order_items_summary.food_revenue,
        order_items_summary.drink_revenue,
        order_items_summary.total_revenue,

        order_items_summary.food_cost,
        order_items_summary.drink_cost,
        order_items_summary.total_cost,

        order_items_summary.food_profit,
        order_items_summary.drink_profit,
        order_items_summary.total_profit

    from orders

    inner join order_items_summary using (order_id)

)

select * from joined 