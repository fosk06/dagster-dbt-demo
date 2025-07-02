MODEL (
  name sqlmesh_jaffle_platform.tweets,
  kind FULL,
  cron '@daily',
  grain id,
);

select * from sqlmesh_jaffle_platform.stg_tweets 