import dagster as dg
import yaml
from pathlib import Path

def build_sling_raw_assets(replication_yaml_path):
    with open(replication_yaml_path, "r") as f:
        config = yaml.safe_load(f)

    assets = []
    for stream_path in config.get("streams", {}):
        if stream_path.startswith("file://"):
            rel_path = stream_path.replace("file://", "")
            p = Path(rel_path)
            stem = p.stem
            key = ["file_", f"_jaffle_data_{stem}", "csv"]
            assets.append(
                dg.AssetSpec(key=key, group_name="ingestion")
            )
    return assets

defs = dg.Definitions(
    assets=build_sling_raw_assets("src/jaffle_platform/defs/ingest_files/replication.yaml")
)