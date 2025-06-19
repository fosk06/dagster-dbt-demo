
    
    

select
    id as unique_field,
    count(*) as n_records

from "jaffle_platform"."main"."raw_items"
where id is not null
group by id
having count(*) > 1


