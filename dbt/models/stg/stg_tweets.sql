with source as (
    select * from {{ source('main', 'raw_tweets') }}
)

select
    id,
    user_id,
    cast(tweeted_at as timestamp) as tweeted_at,
    content
from source 