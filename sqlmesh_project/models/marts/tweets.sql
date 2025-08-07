MODEL (
  name sqlmesh_jaffle_platform.tweets,
  kind FULL,
  tags ["dagster:group_name:datamarts_sqlmesh", "datamarts"],
  cron '@daily',
  grain id,
);

select * from sqlmesh_jaffle_platform.stg_tweets 