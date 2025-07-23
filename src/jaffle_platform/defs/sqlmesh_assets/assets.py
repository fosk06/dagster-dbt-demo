from dagster import Definitions
from .resource import SQLMeshResource
import dagster as dg
import pandas as pd
from .decorators import sqlmesh_multi_asset

@sqlmesh_multi_asset(
    sqlmesh_resource=SQLMeshResource(project_dir="sqlmesh_project", target="dev"),
    name="sqlmesh_multi_asset",
    group_name="sqlmesh",
)
def sqlmesh_assets(context: dg.AssetExecutionContext, sqlmesh: SQLMeshResource):
    metadata_by_key = context.assets_def.metadata_by_key
    for asset_key, metadata in metadata_by_key.items():
        model = metadata["sqlmesh_model"]
        translator = metadata["sqlmesh_translator"]
        # Exécution du modèle SQLMesh (ici, on utilise evaluate, adapte selon ton besoin)
        result = sqlmesh.evaluate(model)
        # On yield le résultat (DataFrame) pour chaque asset
        yield {asset_key: result}


defs = Definitions(
    assets=[sqlmesh_assets],
    resources={
        "sqlmesh": SQLMeshResource(project_dir="sqlmesh_project", target="dev"),
    },
)