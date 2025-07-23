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

    return dg.multi_asset(
        name=name,
        group_name=group_name,
        specs=[
            dg.AssetSpec(
                key=translator.get_asset_key(model),
                deps=translator.get_deps_from_model(model),
                metadata={
                    "sqlmesh_model": model,
                    "sqlmesh_translator": translator,
                },
            )
            for model in models
        ],
    ) 