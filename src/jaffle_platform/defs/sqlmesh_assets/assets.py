from dagster import Definitions
from .resource import SQLMeshResource
import dagster as dg
import pandas as pd


@dg.asset(
    ins={"products": dg.AssetIn(key_prefix=["target", "main"])},
    description="Fake asset",
    compute_kind="python",
    group_name="sqlmesh",
)
def fake_asset(
    context: dg.AssetExecutionContext,
    products: pd.DataFrame,
    sqlmesh: SQLMeshResource,
) -> pd.DataFrame:
    context.log.info(f"SQLMesh project dir: {sqlmesh.project_dir}")
    context.log.info(f"SQLMesh target: {sqlmesh.target}")
    context.log.info(f"SQLMesh models: {list(sqlmesh.get_models())}")
    models = sqlmesh.get_models()
    context.log.info(f"SQLMesh models: {models}")
    return products

defs = Definitions(
    assets=[fake_asset],
    resources={
        "sqlmesh": SQLMeshResource(project_dir="sqlmesh_project", target="dev"),
    },
)