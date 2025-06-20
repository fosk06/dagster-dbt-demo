# from pathlib import Path

# from dagster_dbt import (
#     DbtCliResource,
#     DbtProject,
#     dbt_assets,
# )

# import dagster as dg

# # Get the project root (where pyproject.toml is)
# PROJECT_ROOT = Path(__file__).parents[4]
# DBT_PROJECT_DIR = PROJECT_ROOT / "dbt"

# dbt_project = DbtProject(
#     project_dir=DBT_PROJECT_DIR,
# )
# dbt_project.prepare_if_dev()

# @dbt_assets(
#     manifest=dbt_project.manifest_path
# )
# def all_dbt_assets(context: dg.AssetExecutionContext, dbt: DbtCliResource):
#     yield from dbt.cli(["build"], context=context).stream()


# defs = dg.Definitions(
#     assets=[all_dbt_assets],
#     resources={
#         "dbt": DbtCliResource(project_dir=dbt_project),
#     },
# )