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
    models_to_materialize = sqlmesh.get_models_to_materialize(selected_asset_keys)
    plan = sqlmesh.materialize_assets(models_to_materialize)
    plan_metadata = sqlmesh.extract_plan_metadata(plan)
    for asset_key in selected_asset_keys:
        yield dg.AssetMaterialization(
            asset_key=asset_key,
            metadata=plan_metadata,
        )


defs = Definitions(
    assets=[sqlmesh_assets],
    resources={
        "sqlmesh": SQLMeshResource(project_dir="sqlmesh_project", target="dev"),
    },
)