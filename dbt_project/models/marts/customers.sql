with customers as (

    select * from {{ ref('stg_customers') }}

),

orders as (

    select * from {{ ref('orders') }}

),

customer_orders as (

    select
        customer_id,
        
        min(order_date) as first_order_at,
        max(order_date) as most_recent_order_at,
        count(*) as number_of_orders,

        sum(food_items_count) as total_food_items,
        sum(drink_items_count) as total_drink_items,
        sum(total_items_count) as total_items,

        {{ cents_to_dollars('sum(food_revenue * 100)') }} as total_food_revenue,
        {{ cents_to_dollars('sum(drink_revenue * 100)') }} as total_drink_revenue,
        {{ cents_to_dollars('sum(total_revenue * 100)') }} as total_revenue,

        {{ cents_to_dollars('sum(food_cost * 100)') }} as total_food_cost,
        {{ cents_to_dollars('sum(drink_cost * 100)') }} as total_drink_cost,
        {{ cents_to_dollars('sum(total_cost * 100)') }} as total_cost,

        {{ cents_to_dollars('sum(food_profit * 100)') }} as total_food_profit,
        {{ cents_to_dollars('sum(drink_profit * 100)') }} as total_drink_profit,
        {{ cents_to_dollars('sum(total_profit * 100)') }} as total_profit,

        {{ cents_to_dollars('sum(total_revenue * 100) / nullif(count(*), 0)') }} as avg_order_value,
        {{ cents_to_dollars('sum(total_profit * 100) / nullif(count(*), 0)') }} as avg_order_profit

    from orders
    group by 1

),

joined as (

    select
        customers.customer_id,
        customers.customer_name,

        customer_orders.first_order_at,
        customer_orders.most_recent_order_at,
        customer_orders.number_of_orders,

        customer_orders.total_food_items,
        customer_orders.total_drink_items,
        customer_orders.total_items,

        customer_orders.total_food_revenue,
        customer_orders.total_drink_revenue,
        customer_orders.total_revenue,

        customer_orders.total_food_cost,
        customer_orders.total_drink_cost,
        customer_orders.total_cost,

        customer_orders.total_food_profit,
        customer_orders.total_drink_profit,
        customer_orders.total_profit,

        customer_orders.avg_order_value,
        customer_orders.avg_order_profit

    from customers

    left join customer_orders using (customer_id)

)

select * from joined 