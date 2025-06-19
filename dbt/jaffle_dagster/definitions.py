from dagster import Definitions
from dagster_dbt import DbtCliResource
from .assets import jdbt_dbt_assets
from .project import jdbt_project
from .schedules import schedules

defs = Definitions(
    assets=[jdbt_dbt_assets],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=jdbt_project),
    },
)