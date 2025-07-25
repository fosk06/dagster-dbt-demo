import dagster as dg
from .translator import SQLMeshTranslator
from .resource import SQLMeshResource
from .sqlmesh_asset_utils import get_assetkey_to_snapshot

def sqlmesh_assets_factory(
    *,
    sqlmesh_resource: SQLMeshResource,
    name: str = "sqlmesh_assets",
    group_name: str = "sqlmesh",
    translator: SQLMeshTranslator = None,
    op_tags: dict = None,
    required_resource_keys: set = None,
    retry_policy: dg.RetryPolicy = None,
):
    """
    Factory that returns a Dagster multi_asset for all SQLMesh models, with minimal user code.
    op_tags: Optional dict of tags to attach to the Dagster op (visible in the UI)
    required_resource_keys: Optional set of resource keys to require for the op
    retry_policy: Optional Dagster RetryPolicy to control retries on failure
    """
    translator = translator or SQLMeshTranslator()
    models = list(sqlmesh_resource.get_models())
    assetkey_to_snapshot = get_assetkey_to_snapshot(sqlmesh_resource.context, translator)
    extra_keys = ["cron", "tags", "kind", "dialect", "query", "partitioned_by", "clustered_by"]

    specs = []
    for model in models:
        asset_key = translator.get_asset_key(model)
        snapshot = assetkey_to_snapshot.get(asset_key)
        code_version = str(getattr(snapshot, "version", None)) if snapshot else None # based on the snapshot version
        specs.append(
            dg.AssetSpec(
                key=asset_key,
                deps=translator.get_deps_from_model(model),
                code_version=code_version,
                kinds={"sqlmesh"},
                metadata={
                    "dagster/table_schema": translator.get_table_metadata(model).column_schema,
                    "dagster/table_name": translator.get_table_metadata(model).table_name,
                    "sqlmesh_snapshot_version": code_version,
                    **translator.serialize_metadata(model, extra_keys),
                },
            )
        )

    @dg.multi_asset(
        name=name,
        group_name=group_name,
        specs=specs,
        op_tags=op_tags,
        required_resource_keys=required_resource_keys,
        retry_policy=retry_policy,
    )
    def _sqlmesh_assets(context: dg.AssetExecutionContext, sqlmesh: SQLMeshResource):
        yield from sqlmesh.materialize_all_assets(context)

    return _sqlmesh_assets 