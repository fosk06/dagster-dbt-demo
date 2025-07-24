import dagster as dg
from .translator import SQLMeshTranslator


def sqlmesh_multi_asset(
    *,
    sqlmesh_resource,
    name: str = "sqlmesh_assets",
    group_name: str = "sqlmesh",
    translator: SQLMeshTranslator = None,
):
    translator = translator or SQLMeshTranslator()
    models = list(sqlmesh_resource.get_models())
    assetkey_to_snapshot = sqlmesh_resource.get_assetkey_to_snapshot()
    extra_keys = ["cron", "tags", "kind", "dialect", "query", "partitioned_by", "clustered_by"]

    return dg.multi_asset(
        name=name,
        group_name=group_name,
        specs=[
            dg.AssetSpec(
                key=translator.get_asset_key(model),
                deps=translator.get_deps_from_model(model),
                # code_version=assetkey_to_snapshot.get(translator.get_asset_key(model)).version if assetkey_to_snapshot.get(translator.get_asset_key(model)) else None,
                code_version="1",
                metadata={
                    "dagster/table_schema": translator.get_table_metadata(model).column_schema,
                    "dagster/table_name": translator.get_table_metadata(model).table_name,
                    **translator.serialize_metadata(model, extra_keys),
                },
            )
            for model in models
        ],
    ) 