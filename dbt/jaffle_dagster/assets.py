from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

from .project import jdbt_project


@dbt_assets(manifest=jdbt_project.manifest_path)
def jdbt_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
    