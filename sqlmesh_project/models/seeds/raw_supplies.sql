MODEL (
  name sqlmesh_jaffle_platform.raw_supplies,
  kind SEED (
    path '../../../jaffle-data/raw_supplies.csv'
  ),
  columns (
    id TEXT,
    name TEXT,
    cost INTEGER,
    perishable BOOLEAN,
    sku TEXT
  ),
  grain (id, sku)
); 