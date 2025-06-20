import dagster as dg
import pandas as pd
from ..assets.product_sentiment_scores import product_sentiment_scores

@dg.asset_check(asset=product_sentiment_scores, blocking=True)
def product_sentiment_scores_simple_checks(
    context: dg.AssetCheckExecutionContext,
    product_sentiment_scores: pd.DataFrame
) -> dg.AssetCheckResult:
    results = {}

    # 1. La table ne doit pas être vide
    results['row_count'] = len(product_sentiment_scores) > 0

    # 2. Colonnes requises présentes
    required_columns = ['product_name', 'avg_sentiment', 'mention_count', 'sentiment_category']
    results['required_columns_present'] = all(col in product_sentiment_scores.columns for col in required_columns)

    # 3. Pas de valeurs manquantes dans product_name
    results['no_null_product_name'] = (
        product_sentiment_scores['product_name'].notnull().all()
        if 'product_name' in product_sentiment_scores.columns else False
    )

    # 4. product_name unique
    results['unique_product_name'] = (
        product_sentiment_scores['product_name'].is_unique
        if 'product_name' in product_sentiment_scores.columns else False
    )

    # 5. avg_sentiment entre -1 et 1
    results['sentiment_in_range'] = (
        product_sentiment_scores['avg_sentiment'].between(-1, 1).all()
        if 'avg_sentiment' in product_sentiment_scores.columns else False
    )

    # 6. mention_count >= 1
    results['mention_count_valid'] = (
        (product_sentiment_scores['mention_count'] >= 1).all()
        if 'mention_count' in product_sentiment_scores.columns else False
    )

    # 7. sentiment_std >= 0
    results['sentiment_std_valid'] = (
        (product_sentiment_scores['sentiment_std'] >= 0).all()
        if 'sentiment_std' in product_sentiment_scores.columns else False
    )

    # 8. sentiment_category dans liste valide
    valid_categories = ['Very Negative', 'Negative', 'Neutral', 'Positive', 'Very Positive']
    results['sentiment_category_valid'] = (
        product_sentiment_scores['sentiment_category'].isin(valid_categories).all()
        if 'sentiment_category' in product_sentiment_scores.columns else False
    )

    # Résultat global
    passed = all(results.values())

    # Log des erreurs
    if not passed:
        for check, ok in results.items():
            if not ok:
                context.log.error(f"Check failed: {check}")
    else:
        context.log.info("All simple checks passed")

    serializable_results = {k: bool(v) for k, v in results.items()}

    return dg.AssetCheckResult(passed=passed, metadata={"simple_checks": serializable_results})