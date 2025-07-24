from dagster import Definitions, AssetMaterialization
from .resource import SQLMeshResource
from .translator import SQLMeshTranslator
import dagster as dg
import pandas as pd
from .decorators import sqlmesh_multi_asset

@sqlmesh_multi_asset(
    sqlmesh_resource=SQLMeshResource(project_dir="sqlmesh_project", target="dev"),
    name="sqlmesh_multi_asset",
    group_name="sqlmesh",
)
def sqlmesh_assets(context: dg.AssetExecutionContext, sqlmesh: SQLMeshResource):
    selected_asset_keys = context.selected_asset_keys
    models = list(sqlmesh.get_models())
    metadata_by_key = context.assets_def.metadata_by_key
    translator = SQLMeshTranslator()
    assetkey_to_model = translator.get_assetkey_to_model(models)
    models_to_materialize = [
        assetkey_to_model[asset_key]
        for asset_key in selected_asset_keys
        if asset_key in assetkey_to_model
    ]
    plan = sqlmesh.materialize_assets(models_to_materialize)
    for asset_key in selected_asset_keys:
        yield AssetMaterialization(
            asset_key=asset_key,
            metadata={
                "sqlmesh_plan_id": getattr(plan, "plan_id", None),
                "sqlmesh_environment": str(getattr(plan, "environment", None)),
                "sqlmesh_start": str(getattr(plan, "start", None)),
                "sqlmesh_end": str(getattr(plan, "end", None)),
                "sqlmesh_has_changes": getattr(plan, "has_changes", None),
                "sqlmesh_models_to_backfill": str(getattr(plan, "models_to_backfill", None)),
                "sqlmesh_requires_backfill": getattr(plan, "requires_backfill", None),
                "sqlmesh_modified_snapshots": str(getattr(plan, "modified_snapshots", None)),
                "sqlmesh_user_provided_flags": str(getattr(plan, "user_provided_flags", None)),
            },
        )


defs = Definitions(
    assets=[sqlmesh_assets],
    resources={
        "sqlmesh": SQLMeshResource(project_dir="sqlmesh_project", target="dev"),
    },
)