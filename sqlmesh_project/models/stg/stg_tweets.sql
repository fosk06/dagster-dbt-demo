MODEL (
  name sqlmesh_jaffle_platform.stg_tweets,
  kind FULL,
  cron '@daily',
  grain tweet_id,
  tags ["dagster:group_name:staging_sqlmesh"]
);

with source as (
    select * from main.raw_source_tweets
)

select
    id,
    user_id,
    cast(tweeted_at as timestamp) as tweeted_at,
    content
from source 