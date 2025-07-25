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
):
    """
    Factory that returns a Dagster multi_asset for all SQLMesh models, with minimal user code.
    """
    translator = translator or SQLMeshTranslator()
    models = list(sqlmesh_resource.get_models())
    assetkey_to_snapshot = get_assetkey_to_snapshot(sqlmesh_resource.context, translator)
    extra_keys = ["cron", "tags", "kind", "dialect", "query", "partitioned_by", "clustered_by"]

    @dg.multi_asset(
        name=name,
        group_name=group_name,
        specs=[
            dg.AssetSpec(
                key=translator.get_asset_key(model),
                deps=translator.get_deps_from_model(model),
                code_version="1",
                kinds={"sqlmesh"},
                metadata={
                    "dagster/table_schema": translator.get_table_metadata(model).column_schema,
                    "dagster/table_name": translator.get_table_metadata(model).table_name,
                    **translator.serialize_metadata(model, extra_keys),
                },
            )
            for model in models
        ],
    )
    def _sqlmesh_assets(context: dg.AssetExecutionContext, sqlmesh: SQLMeshResource):
        yield from sqlmesh.materialize_all_assets(context)

    return _sqlmesh_assets 