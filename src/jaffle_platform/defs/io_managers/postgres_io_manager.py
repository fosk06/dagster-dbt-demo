from dagster import io_manager, IOManager, Definitions, AssetObservation, MaterializeResult
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
        Store the output DataFrame in PostgreSQL table named after the asset key.
        
        Note: AssetObservation objects never reach this method - they're handled
        by Dagster's event system. This method only receives actual data objects.
        """
        table_name = context.asset_key.path[-1]
        
        # Vérifier si c'est un MaterializeResult avec value=None ou vide
        if isinstance(obj, MaterializeResult):
            if hasattr(obj, 'value') and obj.value is None:
                context.log.info(f"MaterializeResult for '{table_name}' has no data - skipping storage")
                return
            # Si le MaterializeResult contient de la data, l'extraire
            if hasattr(obj, 'value') and obj.value is not None:
                obj = obj.value
        
        # Si on arrive ici avec None, skip
        if obj is None:
            context.log.info(f"No data to store for '{table_name}' - skipping storage")
            return
            
        # Vérifier que c'est bien un DataFrame
        if not isinstance(obj, pd.DataFrame):
            context.log.warning(f"Expected DataFrame for '{table_name}', got {type(obj)} - skipping storage")
            return
        
        # Stocker le DataFrame
        try:
            with self._get_engine().begin() as conn:
                obj.to_sql(table_name, conn, schema="main", if_exists="replace", index=False)
            context.log.info(f"✅ Stored asset '{table_name}' in PostgreSQL (main.{table_name}) - {len(obj)} rows")
        except Exception as e:
            context.log.error(f"❌ Failed to store '{table_name}': {e}")
            raise

    def load_input(self, context):
        """
        Load DataFrame from PostgreSQL table. Returns None if table doesn't exist.
        """
        table_name = context.asset_key.path[-1]
        
        try:
            with self._get_engine().begin() as conn:
                # Vérifier si la table existe
                exists_query = """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'main' 
                    AND table_name = %s
                )
                """
                table_exists = conn.execute(exists_query, (table_name,)).scalar()
                
                if not table_exists:
                    context.log.warning(f"Table 'main.{table_name}' doesn't exist - returning empty DataFrame")
                    return pd.DataFrame()  # ou lever une exception selon ton besoin
                
                df = pd.read_sql(f'SELECT * FROM main.{table_name}', conn)
                context.log.info(f"✅ Loaded asset '{table_name}' from PostgreSQL - {len(df)} rows")
                return df
                
        except Exception as e:
            context.log.error(f"❌ Failed to load '{table_name}': {e}")
            raise

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