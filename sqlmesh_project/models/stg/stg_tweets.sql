MODEL (
  name sqlmesh_jaffle_platform.stg_tweets,
  kind FULL,
  cron '@daily',
  grain tweet_id,
);

with source as (
    select * from sqlmesh_jaffle_platform.raw_tweets
)

select
    id,
    user_id,
    cast(tweeted_at as timestamp) as tweeted_at,
    content
from source 