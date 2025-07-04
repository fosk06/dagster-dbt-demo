from dagster import (
    AssetExecutionContext,
)
from dagster_sqlmesh import sqlmesh_assets, SQLMeshContextConfig, SQLMeshResource, SQLMeshDagsterTranslator
from dagster_sqlmesh.utils import create_sqlmesh_definitions


sqlmesh_config = SQLMeshContextConfig(path="sqlmesh_project", gateway="postgres", cron_schedule="@daily")

@sqlmesh_assets(environment="prod", config=sqlmesh_config, dagster_sqlmesh_translator=SQLMeshDagsterTranslator())
def sqlmesh_project(context: AssetExecutionContext, sqlmesh: SQLMeshResource):
    yield from sqlmesh.run(context)


defs = create_sqlmesh_definitions(
    config=sqlmesh_config,
    asset_fn=sqlmesh_project,
)