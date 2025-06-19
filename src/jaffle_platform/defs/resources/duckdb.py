import dagster as dg
from typing import Sequence
from dagster_duckdb import DuckDBIOManager
from dagster_duckdb_pandas import DuckDBPandasTypeHandler


class JaffleDuckDBIOManager(DuckDBIOManager):
    @staticmethod
    def type_handlers() -> Sequence[DuckDBPandasTypeHandler]:
        return [DuckDBPandasTypeHandler()]


# L'instance sera créée dans le fichier __init__.py
duckdb_io_manager = JaffleDuckDBIOManager(
    database="/tmp/jaffle_platform.duckdb",
    schema="main"
)

@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "duckdb_io_manager": duckdb_io_manager
        }
    )
