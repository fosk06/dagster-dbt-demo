
    
    

select
    id as unique_field,
    count(*) as n_records

from "jaffle_platform"."main"."raw_customers"
where id is not null
group by id
having count(*) > 1


