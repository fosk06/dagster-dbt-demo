from datetime import timedelta
import dagster as dg

# AssetKeys for all datamart assets
asset_keys = [
    dg.AssetKey(["target", "main", "tweets"]),
    dg.AssetKey(["target", "main", "products"]),
    dg.AssetKey(["target", "main", "orders"]),
    dg.AssetKey(["target", "main", "order_items"]),
    dg.AssetKey(["target", "main", "customers"]),
    dg.AssetKey(["target", "main", "supplies"]),
]

freshness_checks = dg.build_last_update_freshness_checks(
    assets=asset_keys,
    lower_bound_delta=timedelta(minutes=3),
    # deadline_cron="*/5 * * * *"
)

freshness_sensor = dg.build_sensor_for_freshness_checks(
    freshness_checks=freshness_checks
)

defs = dg.Definitions(
    asset_checks=freshness_checks,
    sensors=[freshness_sensor],
)