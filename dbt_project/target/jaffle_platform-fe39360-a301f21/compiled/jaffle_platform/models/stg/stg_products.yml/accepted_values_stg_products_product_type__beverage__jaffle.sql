
    
    

with all_values as (

    select
        product_type as value_field,
        count(*) as n_records

    from "jaffle_platform"."main"."stg_products"
    group by product_type

)

select *
from all_values
where value_field not in (
    'beverage','jaffle'
)


