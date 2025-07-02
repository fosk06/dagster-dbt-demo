MODEL (
  name sqlmesh_jaffle_platform.raw_items,
  kind SEED (
    path '../../../jaffle-data/raw_items.csv'
  ),
  columns (
    id TEXT,
    order_id TEXT,
    sku TEXT
  ),
  grain (id)
); 