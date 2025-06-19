
    
    

select
    sku as unique_field,
    count(*) as n_records

from "jaffle_platform"."main"."raw_products"
where sku is not null
group by sku
having count(*) > 1


