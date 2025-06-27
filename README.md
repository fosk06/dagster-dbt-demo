# Jaffle Platform

## Architecture and Updates

- **Monkey patch of external Sling assets**:
  - Dynamic generation of `AssetSpec` for files ingested by Sling, using a Python script that reads the YAML config and creates assets with the correct group_name.
- **Clear separation of the 3 layers**:
  - `ingestion`: ingestion of CSV files via Sling, exposed as external assets and declared as dbt sources.
  - `staging`: dbt staging models (`stg_`), one per source, grouped under "staging".
  - `datamart`: final dbt models (mart), one per business entity, grouped under "datamarts".
- **No cross-references between layers**:
  - Each layer only references the previous one (e.g., mart → staging, staging → ingestion), never directly between marts or between assets of different layers.
- **Addition of Python assets**:
  - External Sling assets are declared dynamically in Python.
- **Addition of asset checks for CSV sources**:
  - Python checks are added on ingestion assets to validate source data quality (uniqueness, not_null, etc.).

---

This project is a demonstration of a modern data platform using:

- [Dagster](https://dagster.io/) for orchestration
- [DuckDB](https://duckdb.org/) for data storage and processing
- [DBT](https://www.getdbt.com/) for data transformation

## Installation

### Prerequisites

- Python 3.11+
- [uv](https://github.com/astral-sh/uv) for dependency management

### Installing dependencies

```bash
# Install uv (if not already installed)
pip install uv

# Sync dependencies with uv
uv sync

# If you have issues with uv, you can force reinstall
uv pip install -r requirements.txt --force-reinstall
```

If you encounter issues with `uv sync`, you can also use `pip`:

```bash
pip install -r requirements.txt
```

## Project Overview

The project simulates a restaurant chain "Jaffle Shop" (a jaffle is an Australian grilled sandwich). It tracks orders, customers, products, and supplies for this chain.

### Data Structure

#### Sources (schema `raw`)

- `raw_customers`: Customers of the chain
  - `id`: Unique customer identifier
  - `name`: Customer name
- `raw_orders`: Orders placed
  - `id`: Unique order identifier
  - `customer_id`: ID of the customer who placed the order
  - `ordered_at`: Order date and time
  - `store_id`: Restaurant ID
  - `status`: Order status (completed)
- `raw_items`: Order items
  - `id`: Unique item identifier
  - `order_id`: Order ID
  - `product_id`: Ordered product ID
- `raw_products`: Available products
  - `sku`: Unique product identifier
  - `name`: Product name
  - `type`: Product type (jaffle/beverage)
  - `price`: Sale price
- `raw_stores`: Restaurants
  - `id`: Unique restaurant identifier
  - `name`: Restaurant name
  - `opened_at`: Opening date
  - `tax_rate`: Applicable tax rate
- `raw_supplies`: Supplies
  - `id`: Unique supply identifier
  - `name`: Supply name
  - `product_id`: Related product ID
  - `supply_cost`: Supply cost
  - `perishable`: Whether the supply is perishable
- `raw_tweets`: Tweets
  - `id`: Unique tweet identifier
  - `text`: Tweet text
  - `tweeted_at`: Tweet date and time

#### Transformed Models

##### Staging (views)

- `stg_customers`: Cleaned customer data
- `stg_orders`: Cleaned order data
- `stg_order_items`: Cleaned item data
- `stg_products`: Cleaned product data
- `stg_stores`: Cleaned store data
- `stg_supplies`: Cleaned supply data

##### Marts (tables)

- `customers`: Enriched customer view
  - Basic customer info
  - Order stats (count, avg value)
  - Product stats (food vs beverage)
  - Total amounts (revenue, costs)
- `orders`: Enriched order view
  - Detailed order info
  - Totals (items, revenue, costs, profit)
  - Links to customers and stores
- `order_items`: Enriched item view
  - Product details
  - Sale price and supply cost
  - Profit per item
- `products`: Enriched product view
  - Basic product info
  - Product type (food/beverage)
  - Sale price
- `supplies`: Enriched supply view
  - Basic supply info
  - Cost and perishability
  - Link to product

## Technical Architecture

1. **Ingestion**: Data is loaded into DuckDB via Sling (Dagster)
2. **Transformation**: DBT is used to create analytical models
3. **Orchestration**: Dagster orchestrates the entire pipeline with a daily schedule

## Tests

The project includes 85 DBT tests:

- Primary key uniqueness tests
- Not-null tests on required fields
- Relationship tests between tables
- Value range tests (positive amounts)
- Accepted values tests (product types, order statuses)

## Configuration

The project uses the following configurations:

- Staging models are materialized as views
- Mart models are materialized as tables
- The default schema is `main`
- The timezone is set to Europe/Paris

## Querying Data

To query data with DuckDB, you have two options:

### 1. Interactive mode

```bash
# From the dbt/jdbt folder
duckdb /tmp/jaffle_platform.duckdb
```

### 2. Command line mode (recommended)

```bash
# General syntax
duckdb /tmp/jaffle_platform.duckdb -c "YOUR_QUERY;"

# Examples:

# List available schemas
duckdb /tmp/jaffle_platform.duckdb -c "SHOW SCHEMAS;"

# List tables and views in the main schema
duckdb /tmp/jaffle_platform.duckdb -c "SHOW TABLES IN main;"

# Top 10 customers by total spent
duckdb /tmp/jaffle_platform.duckdb -c "
SELECT
    customer_name,
    total_revenue,
    number_of_orders,
    avg_order_value
FROM main.customers
ORDER BY total_revenue DESC
LIMIT 10;"

# Sales by product type and month
duckdb /tmp/jaffle_platform.duckdb -c "
SELECT
    date_trunc('month', order_date) as month,
    product_type,
    count(*) as number_of_items,
    sum(product_price) as total_revenue,
    sum(gross_profit_per_item) as total_profit
FROM main.order_items
GROUP BY 1, 2
ORDER BY 1, 2;"

# Restaurant performance
duckdb /tmp/jaffle_platform.duckdb -c "
SELECT
    s.store_name,
    count(DISTINCT o.order_id) as number_of_orders,
    sum(o.total_revenue) as total_revenue,
    sum(o.total_profit) as total_profit,
    avg(o.total_items_count) as avg_items_per_order
FROM main.orders o
JOIN main.stg_stores s ON o.store_id = s.store_id
GROUP BY 1
ORDER BY total_revenue DESC;"

# Most profitable products
duckdb /tmp/jaffle_platform.duckdb -c "
SELECT
    product_name,
    product_type,
    count(*) as times_ordered,
    avg(gross_profit_per_item) as avg_profit_per_item,
    sum(gross_profit_per_item) as total_profit
FROM main.order_items
GROUP BY 1, 2
ORDER BY total_profit DESC
LIMIT 10;"
```

### Data Export

```bash
# Export to CSV
duckdb /tmp/jaffle_platform.duckdb -c "COPY (SELECT * FROM main.customers) TO 'customers.csv';"

# Export to Parquet
duckdb /tmp/jaffle_platform.duckdb -c "COPY (SELECT * FROM main.orders) TO 'orders.parquet' (FORMAT PARQUET);"

# Export to JSON
duckdb /tmp/jaffle_platform.duckdb -c "COPY (SELECT * FROM main.products) TO 'products.json' (FORMAT JSON);"
```

### DuckDB Tips

In interactive mode:

- `.tables` to list all tables
- `.schema TABLE_NAME` to see a table's structure
- `.mode markdown` for markdown display
- `.headers on` to show column headers
- `.quit` to exit

In command line:

```bash
# List tables
duckdb /tmp/jaffle_platform.duckdb -c ".tables"

# See a table's schema
duckdb /tmp/jaffle_platform.duckdb -c ".schema main.customers"

# Enable headers and use markdown mode
duckdb /tmp/jaffle_platform.duckdb -c ".mode markdown" -c ".headers on" -c "SELECT * FROM main.customers LIMIT 5;"
```

## Cheatsheet

### Scaffolding an Asset Check

To create a new asset check for an existing asset, you can use the `dagster scaffold` command. This will generate a boilerplate file for your check.

For example, to create a check for the `product_sentiment_scores` asset:

```bash
dg scaffold defs dagster.asset_check --format=python --asset-key product_sentiment_scores assets_checks/product_sentiment_scores.py
```

## Limitations

- **DuckDB lockfile limitations**: Because this project uses DuckDB as the database engine, its lockfile system can prevent multiple materializations from running in parallel. If you attempt to trigger many asset materializations at the same time, you may encounter database lock errors or serialization issues. This is a known limitation of DuckDB and should be considered when designing high-concurrency workflows.

## Data Contract Validation Example: `raw_tweets` Source

This project demonstrates advanced data quality validation using the [Data Contract Specification](https://datacontract.com/) and its open-source tooling.

### Custom Dataset: `raw_tweets`

- The `raw_tweets` dataset is ingested from a parquet file.
- The parquet file is generated using the `tools/parquet_file_generator.py` script.
- The parquet file is generated with correct typing (e.g., `tweeted_at` as a timestamp).

Additionally, a dataset named `raw_tweets_invalid` is provided. This dataset is identical in structure to `raw_tweets`, but it ingests a Parquet file (`raw_tweets_invalid.parquet`) that does **not** conform to the data contract (e.g., all columns are stored as strings, including `tweeted_at`).

This invalid dataset is included specifically to demonstrate what happens when data contract validation fails: running the materialization on `raw_tweets_invalid` will produce explicit errors, making it easy to test and showcase contract enforcement and error handling in the platform.

### Data Contract Validation

- A data contract YAML file describes the expected schema and types for the `raw_tweets` model, following the [Data Contract Specification](https://datacontract.com/).
- The [Data Contract CLI](https://datacontract.com/) is used to validate the Parquet files against the contract.
- If the data matches the contract (e.g., correct types, required fields), validation passes. If not (e.g., wrong type for `tweeted_at`), validation fails with explicit errors.

### Why this matters

- This approach ensures that data ingested into the platform strictly conforms to agreed-upon contracts, improving reliability and trust in downstream analytics.
- The project demonstrates both a passing and a failing case, making it easy to test and showcase data contract enforcement in a modern data stack.

See the `tools/parquet_file_generator.py` script and the `src/jaffle_platform/defs/source_assets/data_contracts/raw_tweets.yaml` contract for implementation details.
