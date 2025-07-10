from dagster import io_manager, IOManager, Definitions
import pandas as pd
from sqlalchemy import create_engine

class PostgresIOManager(IOManager):
    def __init__(self, config):
        self.config = config

    def _get_engine(self):
        url = (
            f"postgresql+psycopg2://{self.config['user']}:{self.config['password']}"
            f"@{self.config['host']}:{self.config['port']}/{self.config['dbname']}"
        )
        return create_engine(url)

    def handle_output(self, context, obj):
        """
        Store the output DataFrame in a PostgreSQL table named after the asset key (in schema 'main').
        """
        table_name = context.asset_key.path[-1]
        with self._get_engine().begin() as conn:
            obj.to_sql(table_name, conn, schema="main", if_exists="replace", index=False)
        context.log.info(f"Stored asset '{table_name}' in PostgreSQL (main.{table_name})")

    def load_input(self, context):
        """
        Load the DataFrame from the PostgreSQL table named after the asset key (in schema 'main').
        """
        table_name = context.asset_key.path[-1]
        with self._get_engine().begin() as conn:
            df = pd.read_sql(f'SELECT * FROM main.{table_name}', conn)
        context.log.info(f"Loaded asset '{table_name}' from PostgreSQL (main.{table_name})")
        return df

@io_manager(
    config_schema={
        "host": str,
        "port": int,
        "dbname": str,
        "user": str,
        "password": str,
    }
)
def postgres_io_manager(init_context):
    """
    Dagster IO Manager factory for PostgreSQL. Stores/loads DataFrames in the 'main' schema.
    """
    return PostgresIOManager(init_context.resource_config)

defs = Definitions(
    resources={
        "io_manager": postgres_io_manager.configured({
            "host": "localhost",
            "port": 5432,
            "dbname": "jaffle_db",
            "user": "jaffle",
            "password": "jaffle",
        }),
    },
)