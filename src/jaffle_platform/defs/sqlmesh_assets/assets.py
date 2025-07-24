from dagster import Definitions, AssetMaterialization
import dagster as dg
import pandas as pd
from .decorators import sqlmesh_multi_asset
from .resource import SQLMeshResource

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
    assetkey_to_snapshot = sqlmesh.get_assetkey_to_snapshot()
    # Use topological order for yielding outputs
    ordered_asset_keys = sqlmesh.get_topologically_sorted_asset_keys(plan, selected_asset_keys)
    for asset_key in ordered_asset_keys:
        snapshot = assetkey_to_snapshot.get(asset_key)
        yield dg.AssetMaterialization(
            asset_key=asset_key,
            metadata={**plan_metadata, "sqlmesh_snapshot_version": getattr(snapshot, "version", None)},
        )
        yield dg.Output(
            value=None,  # Replace with actual value if available
            output_name=asset_key.to_python_identifier(),
            data_version=dg.DataVersion(str(getattr(snapshot, "version", ""))) if snapshot else None,
            metadata={"sqlmesh_snapshot_version": getattr(snapshot, "version", None)}
        )


defs = Definitions(
    assets=[sqlmesh_assets],
    resources={
        "sqlmesh": SQLMeshResource(project_dir="sqlmesh_project", target="dev"),
    },
)