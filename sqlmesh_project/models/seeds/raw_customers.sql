MODEL (
  name sqlmesh_jaffle_platform.raw_customers,
  kind SEED (
    path '../../../jaffle-data/raw_customers.csv'
  ),
  columns (
    id TEXT,
    name TEXT
  ),
  grain (id)
); 