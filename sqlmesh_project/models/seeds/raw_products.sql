MODEL (
  name sqlmesh_jaffle_platform.raw_products,
  kind SEED (
    path '../../../jaffle-data/raw_products.csv'
  ),
  columns (
    sku TEXT,
    name TEXT,
    type TEXT,
    price INTEGER,
    description TEXT
  ),
  grain (sku)
); 