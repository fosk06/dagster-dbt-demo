from dagster import Definitions, RetryPolicy, AssetKey,Backoff
from .decorators import sqlmesh_assets_factory
from .resource import SQLMeshResource
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

# Configuration du resource SQLMesh avec translator custom
sqlmesh_resource = SQLMeshResource(
    project_dir="sqlmesh_project",
    gateway="postgres",
    allow_breaking_changes=True,
    translator=SlingToSqlmeshTranslator(),
    concurrency_limit=1
)

# Configuration des assets SQLMesh avec support des external assets
# Les external assets (comme vos assets Sling) seront automatiquement détectés
# et mappés selon la logique dans le translator
sqlmesh_assets = sqlmesh_assets_factory(
    sqlmesh_resource=sqlmesh_resource,
    name="sqlmesh_multi_asset",
    group_name="sqlmesh",
    op_tags={"team": "data", "env": "prod"},
    retry_policy=RetryPolicy(max_retries=1, delay=30.0, backoff= Backoff.EXPONENTIAL),
)

defs = Definitions(
    assets=[sqlmesh_assets],
    resources={
        "sqlmesh": sqlmesh_resource,
    },
)