
  
    
    

    create  table
      "jaffle_platform"."main"."order_items__dbt_tmp"
  
    as (
      with order_items as (

    select * from "jaffle_platform"."main"."stg_order_items"

),

orders as (

    select * from "jaffle_platform"."main"."stg_orders"

),

products as (

    select * from "jaffle_platform"."main"."stg_products"

),

supplies as (

    select * from "jaffle_platform"."main"."stg_supplies"

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
        
    round(1.0 * products.product_price / 100, 2)
 as product_price,
        products.is_food_item,
        products.is_drink_item,

        
    round(1.0 * order_supplies_summary.supply_cost / 100, 2)
 as supply_cost,

        -- calculated fields
        
    round(1.0 * products.product_price - coalesce(order_supplies_summary.supply_cost, 0) / 100, 2)
 as gross_profit_per_item

    from order_items

    left join orders on order_items.order_id = orders.order_id

    left join products on order_items.product_id = products.product_id

    left join order_supplies_summary
        on order_items.product_id = order_supplies_summary.product_id

)

select * from joined
    );
  
  