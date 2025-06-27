with source as (

    {#-
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    #}
    select * from {{ source('main', 'raw_orders') }}

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