import dlt
from dlt.sources.filesystem import readers
from pathlib import Path

# Chemin vers le fichier CSV à charger
CSV_PATH = "jaffle-data/raw_customers.csv"
BUCKET_URL = str(Path(CSV_PATH).parent)  # "jaffle-data"
FILE_GLOB = str(Path(CSV_PATH).name)     # "raw_customers.csv"

@dlt.resource(primary_key="id")
def raw_customers():
    for row in readers(bucket_url=BUCKET_URL, file_glob=FILE_GLOB).read_csv():
        yield row

@dlt.source
def customers_source():
    return raw_customers