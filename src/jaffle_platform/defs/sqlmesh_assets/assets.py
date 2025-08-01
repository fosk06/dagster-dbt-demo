from dagster import RetryPolicy, AssetKey, Backoff
from .decorators import sqlmesh_definitions_factory
from .translator import SQLMeshTranslator

class SlingToSqlmeshTranslator(SQLMeshTranslator):
    def get_external_asset_key(self, external_fqn: str) -> AssetKey:
        """
        Mapping custom pour les external assets.
        SQLMesh: 'jaffle_db.main.raw_source_customers' → Sling: ['target', 'main', 'raw_source_customers']
        """
        parts = external_fqn.replace('"', '').split('.')
        if len(parts) >= 3:
            catalog, schema, table = parts[0], parts[1], parts[2]
            return AssetKey(['target', 'main', table])
        return AssetKey(['external'] + parts[1:])

# Retry policy commune
sqlmesh_retry_policy = RetryPolicy(max_retries=1, delay=30.0, backoff=Backoff.EXPONENTIAL)

# Factory tout-en-un : tout configuré en une seule ligne !
defs = sqlmesh_definitions_factory(
    project_dir="sqlmesh_project",
    gateway="postgres",
    translator=SlingToSqlmeshTranslator(),
    concurrency_limit=1,
    ignore_cron=True,  # only for testing purposes
    name="sqlmesh_multi_asset",
    group_name="sqlmesh",
    op_tags={"team": "data", "env": "prod"},
    retry_policy=sqlmesh_retry_policy,
)