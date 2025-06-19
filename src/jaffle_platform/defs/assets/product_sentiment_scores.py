import dagster as dg


@dg.asset(group_name="my_group")
def product_sentiment_scores(context: dg.AssetExecutionContext) -> None:
    """Asset that greets you."""
    context.log.info("hi!")