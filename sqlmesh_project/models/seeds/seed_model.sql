MODEL (
  name sqlmesh_jaffle_platform.seed_model,
  tags ["dagster:group_name:staging", "staging"],
  kind SEED (
    path '../../seeds/seed_data.csv'
  ),
  cron '*/5 * * * *',
  columns (
    id INTEGER,
    item_id INTEGER,
    event_date DATE
  ),
  grain (id, event_date)
);
  