from dagster import Definitions
from .decorators import sqlmesh_assets_factory
from .resource import SQLMeshResource

sqlmesh_resource = SQLMeshResource(
    project_dir="sqlmesh_project",
    gateway="postgres",
    config_override=None,
    allow_breaking_changes=True,
)

sqlmesh_assets = sqlmesh_assets_factory(
    sqlmesh_resource=sqlmesh_resource,
    name="sqlmesh_multi_asset",
    group_name="sqlmesh",
    op_tags={"team": "data", "env": "prod"},
)

defs = Definitions(
    assets=[sqlmesh_assets],
    resources={
        "sqlmesh": sqlmesh_resource,
    },
)