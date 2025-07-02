from dagster import (
    AssetExecutionContext,
    Definitions,
)
from dagster_sqlmesh import sqlmesh_assets, SQLMeshContextConfig, SQLMeshResource, SQLMeshDagsterTranslator

sqlmesh_config = SQLMeshContextConfig(path="sqlmesh_project", gateway="duckdb")

@sqlmesh_assets(environment="prod", config=sqlmesh_config, dagster_sqlmesh_translator=SQLMeshDagsterTranslator())
def sqlmesh_project(context: AssetExecutionContext, sqlmesh: SQLMeshResource):
    yield from sqlmesh.run(context)

defs = Definitions(
    assets=[sqlmesh_project],
    resources={
        "sqlmesh": SQLMeshResource(config=sqlmesh_config),
    },
)