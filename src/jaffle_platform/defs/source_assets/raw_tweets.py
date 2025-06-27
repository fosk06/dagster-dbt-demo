import dagster as dg
import pandas as pd
import duckdb
from jaffle_platform.defs.source_assets.data_contracts import test_data_contract

@dg.asset(
    description="Ingests the raw_tweets.parquet file into DuckDB (table main.raw_tweets), with data contract validation.",
    compute_kind="python",
    group_name="ingestion",
    key=dg.AssetKey(["target", "main", "raw_tweets"]),
)
def raw_tweets(context: dg.AssetExecutionContext) -> pd.DataFrame:
    """
    Loads the raw_tweets.parquet file, validates it against the data contract, and materializes it into DuckDB (table main.raw_tweets).
    If the data contract fails, the asset fails and nothing is written.
    """
    # Test data contract before any ingestion
    result = test_data_contract("raw_tweets")
    if not result["success"]:
        context.log.error(f"Data contract failed: {result['errors']}")
        raise Exception(f"Data contract validation failed: {result['errors']}")
    context.log.info("✅ Data contract raw_tweets successfully validated! Ingesting...")
    # Read the Parquet file
    df = pd.read_parquet("jaffle-data/raw_tweets.parquet")
    context.log.info(f"{len(df)} rows loaded from raw_tweets.parquet")
    # Connect to DuckDB and write to the table
    conn = duckdb.connect('/tmp/jaffle_platform.duckdb')
    conn.execute("CREATE SCHEMA IF NOT EXISTS main;")
    conn.execute("DROP TABLE IF EXISTS main.raw_tweets;")
    conn.execute("CREATE TABLE main.raw_tweets AS SELECT * FROM df")
    conn.close()
    return df

@dg.asset(
    description="Ingests the raw_tweets_invalid.parquet file into DuckDB, with data contract validation. Designed to fail the data contract validation.",
    compute_kind="python",
    group_name="ingestion",
    key=dg.AssetKey(["target", "main", "raw_tweets_invalid"]),
)
def raw_tweets_invalid(context: dg.AssetExecutionContext) -> pd.DataFrame:
    """
    Loads the raw_tweets_invalid.parquet file, validates it against the data contract, and materializes it into DuckDB (table main.raw_tweets_invalid).
    If the data contract fails, the asset fails and nothing is written.
    """
    # Test data contract before any ingestion
    result = test_data_contract("raw_tweets_invalid")
    if not result["success"]:
        context.log.error(f"Data contract failed: {result['errors']}")
        raise Exception(f"Data contract validation failed: {result['errors']}")
    context.log.info("✅ Data contract raw_tweets successfully validated! Ingesting...")
    # Read the Parquet file
    df = pd.read_parquet("jaffle-data/raw_tweets_invalid.parquet")
    context.log.info(f"{len(df)} rows loaded from raw_tweets_invalid.parquet")
    # Connect to DuckDB and write to the table
    conn = duckdb.connect('/tmp/jaffle_platform.duckdb')
    conn.execute("CREATE SCHEMA IF NOT EXISTS main;")
    conn.execute("DROP TABLE IF EXISTS main.raw_tweets;")
    conn.execute("CREATE TABLE main.raw_tweets AS SEL885mbmn`m,,=n,KkECT * FROM df")
    conn.close()
    return df