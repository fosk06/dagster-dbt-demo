import dagster as dg
import pandas as pd
import duckdb
from jaffle_platform.defs.source_assets.data_contracts import test_data_contract

@dg.asset(
    description="Ingestion du fichier raw_tweets.csv dans DuckDB (table main.raw_tweets), avec validation data contract.",
    compute_kind="python",
    group_name="ingestion",
    key=dg.AssetKey(["target", "main", "raw_tweets"]),
)
def raw_tweets(context: dg.AssetExecutionContext) -> pd.DataFrame:
    """
    Charge le CSV raw_tweets.csv, valide le data contract, et le matérialise dans DuckDB (table main.raw_tweets).
    Si le data contract échoue, l'asset échoue et rien n'est écrit.
    """
    # Test data contract avant toute ingestion
    result = test_data_contract("raw_tweets")
    if not result["success"]:
        context.log.error(f"Data contract failed: {result['errors']}")
        raise Exception(f"Data contract validation failed: {result['errors']}")
    context.log.info("✅ Data contract raw_tweets validé avec succès ! Ingestion en cours...")
    # Lecture du CSV
    df = pd.read_parquet("jaffle-data/raw_tweets.parquet")
    context.log.info(f"{len(df)} lignes chargées depuis raw_tweets.parquet")
    # Connexion à DuckDB et écriture dans la table
    conn = duckdb.connect('/tmp/jaffle_platform.duckdb')
    conn.execute("CREATE SCHEMA IF NOT EXISTS main;")
    conn.execute("DROP TABLE IF EXISTS main.raw_tweets;")
    conn.execute("CREATE TABLE main.raw_tweets AS SELECT * FROM df")
    conn.close()
    return df