from dagster import AssetExecutionContext, Definitions, AssetSpec, AssetKey
from dagster_dlt import DagsterDltResource, dlt_assets, DagsterDltTranslator
from dagster_dlt.translator import DltResourceTranslatorData
from dlt import pipeline
from jaffle_platform.dlt_sources.filesystem_pipeline import local_csv_source
import os
import psycopg2

# Set DLT PostgreSQL credentials for the pipeline
os.environ["RAW_CUSTOMERS_PIPELINE__DESTINATION__POSTGRES__CREDENTIALS__DATABASE"] = "jaffle_db"
os.environ["RAW_CUSTOMERS_PIPELINE__DESTINATION__POSTGRES__CREDENTIALS__USERNAME"] = "jaffle"
os.environ["RAW_CUSTOMERS_PIPELINE__DESTINATION__POSTGRES__CREDENTIALS__PASSWORD"] = "jaffle"
os.environ["RAW_CUSTOMERS_PIPELINE__DESTINATION__POSTGRES__CREDENTIALS__HOST"] = "localhost"
os.environ["RAW_CUSTOMERS_PIPELINE__DESTINATION__POSTGRES__CREDENTIALS__PORT"] = "5432"

dlt_resource = DagsterDltResource()
dlt_pg_pipeline = pipeline(
    pipeline_name="raw_customers_pipeline",
    destination='postgres',
    dataset_name="main",
)

class DltToDbtTranslator(DagsterDltTranslator):
    def get_asset_spec(self, data: DltResourceTranslatorData) -> AssetSpec:
        """Overrides asset spec to override asset key to be the same as dbt source asset name"""
        default_spec = super().get_asset_spec(data)
        print(f"resource name: {data.resource.name}")
        return default_spec.replace_attributes(
            key=AssetKey(['target', 'main', f"{data.resource.name}"]),  # the dbt component format the keys like this
        )

def truncate_tables(schema, tables):
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="jaffle_db",
        user="jaffle",
        password="jaffle"
    )
    try:
        with conn.cursor() as cur:
            for table in tables:
                print(f"Truncating {schema}.{table} ...", flush=True)
                cur.execute(f'TRUNCATE TABLE "{schema}"."{table}" RESTART IDENTITY CASCADE;')
        conn.commit()
        print("All target tables truncated.")
    finally:
        conn.close()


@dlt_assets(
    dlt_source=local_csv_source(),
    dlt_pipeline=dlt_pg_pipeline,
    dagster_dlt_translator=DltToDbtTranslator(),
    group_name="landing",
)
def dagster_dlt_landings_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    truncate_tables("main", ["raw_customers"])
    yield from dlt.run(context=context)

dlt_source_assets = [
    AssetSpec(key, group_name="landing") for key in dagster_dlt_landings_assets.dependency_keys
]

defs = Definitions(
    assets=[
        dagster_dlt_landings_assets,
        *dlt_source_assets,
    ],
    resources={
        "dlt": dlt_resource,
    },
)