MODEL (
  name sqlmesh_jaffle_platform.raw_orders,
  kind SEED (
    path '../../../jaffle-data/raw_orders.csv'
  ),
  columns (
    id TEXT,
    customer TEXT,
    ordered_at TEXT,
    store_id TEXT,
    subtotal INTEGER,
    tax_paid INTEGER,
    order_total INTEGER
  ),
  grain (id)
); 