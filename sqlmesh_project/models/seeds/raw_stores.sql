MODEL (
  name sqlmesh_jaffle_platform.raw_stores,
  kind SEED (
    path '../../../jaffle-data/raw_stores.csv'
  ),
  columns (
    id TEXT,
    name TEXT,
    opened_at TEXT,
    tax_rate FLOAT
  ),
  grain (id)
); 