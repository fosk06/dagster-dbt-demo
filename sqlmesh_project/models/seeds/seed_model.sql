MODEL (
  name sqlmesh_jaffle_platform.seed_model,
  tags ["dagster:group_name:staging_sqlmesh"],
  kind SEED (
    path '../../seeds/seed_data.csv'
  ),
  columns (
    id INTEGER,
    item_id INTEGER,
    event_date DATE
  ),
  grain (id, event_date)
);
  