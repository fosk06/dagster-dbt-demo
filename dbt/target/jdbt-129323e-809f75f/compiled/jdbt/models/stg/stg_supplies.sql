with source as (

    select * from "jaffle_platform"."main"."raw_supplies"

),

renamed as (

    select
        ----------  ids
        id as supply_id,
        sku as product_id,

        ----------  strings
        name as supply_name,

        ----------  numerics
        cost as supply_cost,

        ----------  booleans
        perishable

    from source

)

select * from renamed