with order_items as (

    select * from {{ ref('stg_order_items') }}

),

orders as (

    select * from {{ ref('stg_orders') }}

),

products as (

    select * from {{ ref('stg_products') }}

),

supplies as (

    select * from {{ ref('stg_supplies') }}

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

        orders.order_date,
        orders.store_id,

        products.product_name,
        {{ cents_to_dollars('products.product_price') }} as product_price,
        products.is_food_item,
        products.is_drink_item,

        {{ cents_to_dollars('order_supplies_summary.supply_cost') }} as supply_cost,

        -- calculated fields
        {{ cents_to_dollars('products.product_price - coalesce(order_supplies_summary.supply_cost, 0)') }} as gross_profit_per_item

    from order_items

    left join orders on order_items.order_id = orders.order_id

    left join products on order_items.product_id = products.product_id

    left join order_supplies_summary
        on order_items.product_id = order_supplies_summary.product_id

)

select * from joined 