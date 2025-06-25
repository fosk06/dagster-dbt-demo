import dlt
from dagster import AssetExecutionContext, Definitions
from dagster_dlt import DagsterDltResource, dlt_assets

@dlt.source
def customers_source():
    @dlt.resource
    def raw_customers():
        import csv
        with open("jaffle-data/raw_customers.csv", newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                yield row
    return raw_customers

customers_pipeline = dlt.pipeline(
    pipeline_name="customers_pipeline",
    destination=dlt.destinations.duckdb("/tmp/jaffle_platform.duckdb"),
    dataset_name="main",
    progress="log",
)

dlt_resource = DagsterDltResource()

@dlt_assets(
    dlt_source=customers_source(),
    dlt_pipeline=customers_pipeline,
)
def customers_dlt_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)

defs = Definitions(
    assets=[customers_dlt_assets],
    resources={"dlt": dlt_resource},
)
