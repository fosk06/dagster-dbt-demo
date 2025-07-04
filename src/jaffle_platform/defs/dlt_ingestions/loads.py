from dagster import AssetExecutionContext, Definitions, AssetSpec, AssetKey
from dagster_dlt import DagsterDltResource, dlt_assets, DagsterDltTranslator
from dagster_dlt.translator import DltResourceTranslatorData
from dlt import pipeline
from jaffle_platform.dlt_sources.filesystem_pipeline import local_csv_source
dlt_resource = DagsterDltResource()

class DltToDbtTranslator(DagsterDltTranslator):
    def get_asset_spec(self, data: DltResourceTranslatorData) -> AssetSpec:
        """Overrides asset spec to override asset key to be the same as dbt source asset name"""
        default_spec = super().get_asset_spec(data)
        print(f"resource name: {data.resource.name}")
        return default_spec.replace_attributes(
            key=AssetKey(['target', 'main', f"{data.resource.name}"]), # the dbt component format the keys like this
        )

@dlt_assets(
    dlt_source=local_csv_source(),
    dlt_pipeline = pipeline(
        pipeline_name="raw_customers_pipeline",
        destination='duckdb',
        dataset_name="main",
    ),
    dagster_dlt_translator=DltToDbtTranslator(),
    group_name="landing",

)
def dagster_dlt_landings_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
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