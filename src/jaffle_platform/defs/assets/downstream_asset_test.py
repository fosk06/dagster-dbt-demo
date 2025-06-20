import dagster as dg
import pandas as pd
import duckdb

@dg.asset(
    description="Un asset de test simple pour vérifier les dépendances.",
    compute_kind="python",
    group_name="default",
    deps=[dg.AssetKey(["target", "main", "raw_tweets"])],
)
def downstream_asset_test(context: dg.AssetExecutionContext) -> pd.DataFrame:
    """
    Cet asset charge simplement l'asset en amont 'raw_tweets' et le retourne.
    """
    conn = duckdb.connect('/tmp/jaffle_platform.duckdb')
    df = conn.execute("SELECT * FROM main.raw_tweets").df()
    conn.close()

    context.log.info(f"L'asset 'raw_tweets' a été chargé avec succès. Il contient {len(df)} lignes.")
    return df