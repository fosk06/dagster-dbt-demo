





with validation_errors as (

    select
        supply_id, product_id
    from "jaffle_platform"."main"."supplies"
    group by supply_id, product_id
    having count(*) > 1

)

select *
from validation_errors


