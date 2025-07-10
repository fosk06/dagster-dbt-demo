import dagster as dg
import pandas as pd
import psycopg2

def load_table_from_postgres(table_name: str) -> pd.DataFrame:
    """
    Utility function to load a table from PostgreSQL as a DataFrame.
    """
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="jaffle_db",
        user="jaffle",
        password="jaffle"
    )
    df = pd.read_sql(f'SELECT * FROM {table_name}', conn)
    conn.close()
    return df

@dg.asset_check(asset=dg.AssetKey(['target', 'main', 'raw_items']), blocking=True)
def raw_items_checks(
    context: dg.AssetCheckExecutionContext,
) -> dg.AssetCheckResult:
    raw_items = load_table_from_postgres('main.raw_items')
    results = {}
    # Column mapping: item_id -> id, product_id -> sku, order_id -> order_id
    # There is no quantity column in the current table
    required_columns = ['id', 'sku', 'order_id']
    results['required_columns_present'] = all(col in raw_items.columns for col in required_columns)
    results['row_count'] = len(raw_items) > 0
    results['no_null_id'] = (
        raw_items['id'].notnull().all() if 'id' in raw_items.columns else False
    )
    results['unique_id'] = (
        raw_items['id'].is_unique if 'id' in raw_items.columns else False
    )
    results['no_null_sku'] = (
        raw_items['sku'].notnull().all() if 'sku' in raw_items.columns else False
    )
    results['no_null_order_id'] = (
        raw_items['order_id'].notnull().all() if 'order_id' in raw_items.columns else False
    )
    passed = all(results.values())
    if not passed:
        for check, ok in results.items():
            if not ok:
                context.log.error(f"Check failed: {check}")
    else:
        context.log.info("All raw_items checks passed")
    serializable_results = {k: bool(v) for k, v in results.items()}
    return dg.AssetCheckResult(passed=passed, metadata={"raw_items_checks": serializable_results})

@dg.asset_check(asset=dg.AssetKey(['target', 'main', 'raw_orders']), blocking=True)
def raw_orders_checks(
    context: dg.AssetCheckExecutionContext,
) -> dg.AssetCheckResult:
    raw_orders = load_table_from_postgres('main.raw_orders')
    results = {}
    required_columns = ['id', 'customer', 'ordered_at']
    results['required_columns_present'] = all(col in raw_orders.columns for col in required_columns)
    results['row_count'] = len(raw_orders) > 0
    results['no_null_id'] = (
        raw_orders['id'].notnull().all() if 'id' in raw_orders.columns else False
    )
    results['unique_id'] = (
        raw_orders['id'].is_unique if 'id' in raw_orders.columns else False
    )
    results['no_null_customer'] = (
        raw_orders['customer'].notnull().all() if 'customer' in raw_orders.columns else False
    )
    results['no_null_ordered_at'] = (
        raw_orders['ordered_at'].notnull().all() if 'ordered_at' in raw_orders.columns else False
    )
    passed = all(results.values())
    if not passed:
        for check, ok in results.items():
            if not ok:
                context.log.error(f"Check failed: {check}")
    else:
        context.log.info("All raw_orders checks passed")
    serializable_results = {k: bool(v) for k, v in results.items()}
    return dg.AssetCheckResult(passed=passed, metadata={"raw_orders_checks": serializable_results})

@dg.asset_check(asset=dg.AssetKey(['target', 'main', 'raw_supplies']), blocking=True)
def raw_supplies_checks(
    context: dg.AssetCheckExecutionContext,
) -> dg.AssetCheckResult:
    raw_supplies = load_table_from_postgres('main.raw_supplies')
    results = {}
    required_columns = ['id', 'name', 'cost', 'perishable', 'sku']
    results['required_columns_present'] = all(col in raw_supplies.columns for col in required_columns)
    results['row_count'] = len(raw_supplies) > 0
    results['no_null_id'] = raw_supplies['id'].notnull().all() if 'id' in raw_supplies.columns else False
    results['no_null_name'] = raw_supplies['name'].notnull().all() if 'name' in raw_supplies.columns else False
    results['no_null_cost'] = raw_supplies['cost'].notnull().all() if 'cost' in raw_supplies.columns else False
    results['no_null_sku'] = raw_supplies['sku'].notnull().all() if 'sku' in raw_supplies.columns else False
    passed = all(results.values())
    if not passed:
        for check, ok in results.items():
            if not ok:
                context.log.error(f"Check failed: {check}")
    else:
        context.log.info("All raw_supplies checks passed")
    serializable_results = {k: bool(v) for k, v in results.items()}
    return dg.AssetCheckResult(passed=passed, metadata={"raw_supplies_checks": serializable_results})

@dg.asset_check(asset=dg.AssetKey(['target', 'main', 'raw_stores']), blocking=True)
def raw_stores_checks(
    context: dg.AssetCheckExecutionContext,
) -> dg.AssetCheckResult:
    raw_stores = load_table_from_postgres('main.raw_stores')
    results = {}
    required_columns = ['id', 'name', 'opened_at', 'tax_rate']
    results['required_columns_present'] = all(col in raw_stores.columns for col in required_columns)
    results['row_count'] = len(raw_stores) > 0
    results['no_null_id'] = raw_stores['id'].notnull().all() if 'id' in raw_stores.columns else False
    results['unique_id'] = raw_stores['id'].is_unique if 'id' in raw_stores.columns else False
    results['no_null_name'] = raw_stores['name'].notnull().all() if 'name' in raw_stores.columns else False
    results['no_null_opened_at'] = raw_stores['opened_at'].notnull().all() if 'opened_at' in raw_stores.columns else False
    results['no_null_tax_rate'] = raw_stores['tax_rate'].notnull().all() if 'tax_rate' in raw_stores.columns else False
    passed = all(results.values())
    if not passed:
        for check, ok in results.items():
            if not ok:
                context.log.error(f"Check failed: {check}")
    else:
        context.log.info("All raw_stores checks passed")
    serializable_results = {k: bool(v) for k, v in results.items()}
    return dg.AssetCheckResult(passed=passed, metadata={"raw_stores_checks": serializable_results})

@dg.asset_check(asset=dg.AssetKey(['target', 'main', 'raw_products']), blocking=True)
def raw_products_checks(
    context: dg.AssetCheckExecutionContext,
) -> dg.AssetCheckResult:
    raw_products = load_table_from_postgres('main.raw_products')
    results = {}
    required_columns = ['sku', 'name', 'type', 'price']
    results['required_columns_present'] = all(col in raw_products.columns for col in required_columns)
    results['row_count'] = len(raw_products) > 0
    results['no_null_sku'] = raw_products['sku'].notnull().all() if 'sku' in raw_products.columns else False
    results['unique_sku'] = raw_products['sku'].is_unique if 'sku' in raw_products.columns else False
    results['no_null_name'] = raw_products['name'].notnull().all() if 'name' in raw_products.columns else False
    results['no_null_type'] = raw_products['type'].notnull().all() if 'type' in raw_products.columns else False
    results['no_null_price'] = raw_products['price'].notnull().all() if 'price' in raw_products.columns else False
    passed = all(results.values())
    if not passed:
        for check, ok in results.items():
            if not ok:
                context.log.error(f"Check failed: {check}")
    else:
        context.log.info("All raw_products checks passed")
    serializable_results = {k: bool(v) for k, v in results.items()}
    return dg.AssetCheckResult(passed=passed, metadata={"raw_products_checks": serializable_results})

@dg.asset_check(asset=dg.AssetKey(['target', 'main', 'raw_customers']), blocking=True)
def raw_customers_checks(
    context: dg.AssetCheckExecutionContext,
) -> dg.AssetCheckResult:
    raw_customers = load_table_from_postgres('main.raw_customers')
    results = {}
    required_columns = ['id', 'name']
    results['required_columns_present'] = all(col in raw_customers.columns for col in required_columns)
    results['row_count'] = len(raw_customers) > 0
    results['no_null_id'] = raw_customers['id'].notnull().all() if 'id' in raw_customers.columns else False
    results['unique_id'] = raw_customers['id'].is_unique if 'id' in raw_customers.columns else False
    results['no_null_name'] = raw_customers['name'].notnull().all() if 'name' in raw_customers.columns else False
    passed = all(results.values())
    if not passed:
        for check, ok in results.items():
            if not ok:
                context.log.error(f"Check failed: {check}")
    else:
        context.log.info("All raw_customers checks passed")
    serializable_results = {k: bool(v) for k, v in results.items()}
    return dg.AssetCheckResult(passed=passed, metadata={"raw_customers_checks": serializable_results})

@dg.asset_check(asset=dg.AssetKey(['target', 'main', 'raw_tweets']), blocking=True)
def raw_tweets_checks(
    context: dg.AssetCheckExecutionContext,
) -> dg.AssetCheckResult:
    raw_tweets = load_table_from_postgres('main.raw_tweets')
    results = {}
    required_columns = ['id', 'user_id', 'tweeted_at', 'content']
    results['required_columns_present'] = all(col in raw_tweets.columns for col in required_columns)
    results['row_count'] = len(raw_tweets) > 0
    results['no_null_id'] = raw_tweets['id'].notnull().all() if 'id' in raw_tweets.columns else False
    results['unique_id'] = raw_tweets['id'].is_unique if 'id' in raw_tweets.columns else False
    results['no_null_user_id'] = raw_tweets['user_id'].notnull().all() if 'user_id' in raw_tweets.columns else False
    results['no_null_tweeted_at'] = raw_tweets['tweeted_at'].notnull().all() if 'tweeted_at' in raw_tweets.columns else False
    results['no_null_content'] = raw_tweets['content'].notnull().all() if 'content' in raw_tweets.columns else False
    passed = all(results.values())
    if not passed:
        for check, ok in results.items():
            if not ok:
                context.log.error(f"Check failed: {check}")
    else:
        context.log.info("All raw_tweets checks passed")
    serializable_results = {k: bool(v) for k, v in results.items()}
    return dg.AssetCheckResult(passed=passed, metadata={"raw_tweets_checks": serializable_results})