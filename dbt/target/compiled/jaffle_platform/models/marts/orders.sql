with orders as (

    select * from "jaffle_platform"."main"."stg_orders"

),

order_items as (

    select * from "jaffle_platform"."main"."order_items"

),

order_items_summary as (

    select
        order_id,
        
        sum(case when is_food_item then 1 else 0 end) as food_items_count,
        sum(case when is_drink_item then 1 else 0 end) as drink_items_count,
        count(*) as total_items_count,

        
    round(1.0 * sum(case when is_food_item then product_price * 100 else 0 end) / 100, 2)
 as food_revenue,
        
    round(1.0 * sum(case when is_drink_item then product_price * 100 else 0 end) / 100, 2)
 as drink_revenue,
        
    round(1.0 * sum(product_price * 100) / 100, 2)
 as total_revenue,

        
    round(1.0 * sum(case when is_food_item then supply_cost * 100 else 0 end) / 100, 2)
 as food_cost,
        
    round(1.0 * sum(case when is_drink_item then supply_cost * 100 else 0 end) / 100, 2)
 as drink_cost,
        
    round(1.0 * sum(supply_cost * 100) / 100, 2)
 as total_cost,

        
    round(1.0 * sum(case when is_food_item then gross_profit_per_item * 100 else 0 end) / 100, 2)
 as food_profit,
        
    round(1.0 * sum(case when is_drink_item then gross_profit_per_item * 100 else 0 end) / 100, 2)
 as drink_profit,
        
    round(1.0 * sum(gross_profit_per_item * 100) / 100, 2)
 as total_profit

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