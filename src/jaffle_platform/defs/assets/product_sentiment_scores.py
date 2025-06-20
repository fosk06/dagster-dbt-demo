import dagster as dg
import pandas as pd

@dg.asset(
    description="Un asset de test simple pour vérifier les dépendances.",
    compute_kind="python",
    group_name="default",
    io_manager_key="duckdb_io_manager",
    deps=[dg.AssetKey(["target", "main", "raw_tweets"])],
)
def downstream_asset_test(context: dg.AssetExecutionContext, raw_tweets: pd.DataFrame) -> pd.DataFrame:
    """
    Cet asset charge simplement l'asset en amont 'raw_tweets' et le retourne.
    """
    context.log.info(f"L'asset 'raw_tweets' a été chargé avec succès. Il contient {len(raw_tweets)} lignes.")
    return raw_tweets