with source as (

    select * from {{ source('main', 'raw_stores') }}

),

renamed as (

    select
        ----------  ids
        id as store_id,

        ----------  strings
        name as store_name,

        ----------  timestamps
        opened_at as opened_at,

        ----------  numerics
        tax_rate

    from source

)

select * from renamed 