import dagster as dg
import yaml
from pathlib import Path
from urllib.parse import urlparse

def build_sling_raw_assets(replication_yaml_path, group_name="landing"):
    with open(replication_yaml_path, "r") as f:
        config = yaml.safe_load(f)

    assets = []
    for stream_path in config.get("streams", {}):
        if stream_path.startswith("file://"):
            parsed = urlparse(stream_path)
            rel_path = parsed.path.lstrip("/")
            p = Path(rel_path)
            parts = list(p.parts)
            if parts:
                if parts[-1].endswith(".csv"):
                    parts[-1] = parts[-1][:-4]
                    file_type = "csv"
                elif parts[-1].endswith(".parquet"):
                    parts[-1] = parts[-1][:-8]
                    file_type = "parquet"
                else:
                    file_type = "unknown"
            joined = "_".join(parts)
            joined = joined.replace("-", "_")
            key = ["file_", f"_{joined}", file_type]
            assets.append(
                dg.AssetSpec(key=key, group_name=group_name)
            )
    return assets

defs = dg.Definitions(
    assets=build_sling_raw_assets("src/jaffle_platform/defs/ingest_files/replication.yaml", group_name="landing")
)