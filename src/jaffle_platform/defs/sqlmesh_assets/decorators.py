import dagster as dg
from .translator import SQLMeshTranslator
from .resource import SQLMeshResource
from .sqlmesh_asset_utils import (
    get_assetkey_to_snapshot,
    get_asset_kinds,
    get_asset_group_name,
    get_asset_tags,
    get_asset_metadata,
)

def sqlmesh_assets_factory(
    *,
    sqlmesh_resource: SQLMeshResource,
    name: str = "sqlmesh_assets",
    group_name: str = "sqlmesh",
    translator: SQLMeshTranslator = None,
    op_tags: dict = None,
    required_resource_keys: set = None,
    retry_policy: dg.RetryPolicy = None,
    owners: list = None,
):
    """
    Factory that returns a Dagster multi_asset for all SQLMesh models, with minimal user code.
    op_tags: Optional dict of tags to attach to the Dagster op (visible in the UI)
    required_resource_keys: Optional set of resource keys to require for the op
    retry_policy: Optional Dagster RetryPolicy to control retries on failure
    owners: Optional list of owners to attach to each asset (for governance)
    """
    translator = translator or SQLMeshTranslator()
    
    # Pre-calculate expensive operations once
    models = list(sqlmesh_resource.get_models())
    context = sqlmesh_resource.context
    assetkey_to_snapshot = get_assetkey_to_snapshot(context, translator)
    extra_keys = ["cron", "tags", "kind", "dialect", "query", "partitioned_by", "clustered_by"]
    
    # Pre-calculate common values
    kinds = get_asset_kinds(translator, context)

    specs = []
    for model in models:
        asset_key = translator.get_asset_key(model)
        snapshot = assetkey_to_snapshot.get(asset_key)
        code_version = str(getattr(snapshot, "version", None)) if snapshot else None
        metadata = get_asset_metadata(translator, model, code_version, extra_keys, owners)
        tags = get_asset_tags(translator, context, model)
        group_name = get_asset_group_name(translator, context, model)
        specs.append(
            dg.AssetSpec(
                key=asset_key,
                deps=translator.get_deps_from_model(model),
                code_version=code_version,
                metadata=metadata,
                kinds=kinds,
                tags=tags,
                group_name=group_name,
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