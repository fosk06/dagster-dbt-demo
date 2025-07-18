import dagster as dg
from dagster.components import load_defs
import jaffle_platform.defs


# ---------------------------------------------------
# Definitions

# Definitions are the collection of assets, jobs, schedules, resources, and sensors
# used with a project. Dagster Cloud deployments can contain mulitple projects.


defs = dg.Definitions.merge(
    load_defs(jaffle_platform.defs),
    dg.Definitions(
        executor=dg.multiprocess_executor.configured({"max_concurrent": 3}),
    ),
)
