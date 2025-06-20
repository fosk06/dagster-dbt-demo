import dagster as dg
import great_expectations as ge
import pandas as pd

@dg.asset_check(asset=dg.AssetKey(['product_sentiment_scores']), blocking=True)
def product_sentiment_scores_ge_checks(context: dg.AssetCheckExecutionContext, product_sentiment_scores: pd.DataFrame) -> dg.AssetCheckResult:
    # Convertir le DataFrame en DataFrame GE
    ge_df = ge.from_pandas(product_sentiment_scores)
    results = {}

    # 1. La table ne doit pas être vide
    results["row_count"] = ge_df.expect_table_row_count_to_be_between(min_value=1)

    # 2. Le nom du produit ne doit jamais être manquant
    results["product_name_not_null"] = ge_df.expect_column_values_to_not_be_null("product_name")

    # 3. Chaque produit ne doit apparaître qu'une seule fois
    results["product_name_unique"] = ge_df.expect_column_values_to_be_unique("product_name")

    # 4. Le score de sentiment moyen doit être entre -1 et 1
    results["avg_sentiment_range"] = ge_df.expect_column_values_to_be_between("avg_sentiment", min_value=-1, max_value=1)

    # 5. Le nombre de mentions doit être au moins de 1
    results["mention_count_min"] = ge_df.expect_column_values_to_be_between("mention_count", min_value=1)

    # 6. L'écart-type du sentiment ne peut pas être négatif
    results["sentiment_std_min"] = ge_df.expect_column_values_to_be_between("sentiment_std", min_value=0)

    # 7. La catégorie de sentiment doit être valide
    results["sentiment_category_valid"] = ge_df.expect_column_values_to_be_in_set(
        "sentiment_category",
        ["Very Negative", "Negative", "Neutral", "Positive", "Very Positive"]
    )

    # 8. Vérification du schéma (présence des colonnes)
    required_columns = ["product_name", "avg_sentiment", "mention_count", "sentiment_category"]
    for col in required_columns:
        results[f"{col}_exists"] = ge_df.expect_table_column_to_exist(col)

    # Synthèse du résultat global
    passed = all(r["success"] for r in results.values())
    # Collecte des détails
    metadata = {
        "great_expectations_results": dg.MetadataValue.json({k: v for k, v in results.items()})
    }

    if not passed:
        context.log.error("Great Expectations checks failed")
    else:
        context.log.info("All Great Expectations checks passed")

    return dg.AssetCheckResult(passed=passed, metadata=metadata)
