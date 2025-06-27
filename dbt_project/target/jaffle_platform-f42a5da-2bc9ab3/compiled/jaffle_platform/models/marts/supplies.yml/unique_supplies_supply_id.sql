
    
    

select
    supply_id as unique_field,
    count(*) as n_records

from "jaffle_platform"."main"."supplies"
where supply_id is not null
group by supply_id
having count(*) > 1


