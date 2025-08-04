MODEL (
  name sqlmesh_jaffle_platform.stg_tweets,
  kind FULL,
  cron '@daily',
  grain id,
  tags ["dagster:group_name:staging_sqlmesh"],
  audits(
    number_of_rows(threshold := 10),
    not_null(columns := (id, user_id))
  )
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