with source as (

    {#-
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    #}
    select * from {{ source('main', 'raw_customers') }}

),

renamed as (

    select
        id as customer_id,
        name as customer_name

    from source

)

select * from renamed