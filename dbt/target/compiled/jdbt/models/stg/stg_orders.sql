with source as (
    select * from "jaffle_platform"."main"."raw_orders"

),

renamed as (

    select
        id as order_id,
        customer as customer_id,
        ordered_at as order_date,
        store_id,
        subtotal,
        tax_paid,
        order_total,
        'completed' as status  -- Assuming all orders are completed since we don't have status in raw data

    from source

)

select * from renamed