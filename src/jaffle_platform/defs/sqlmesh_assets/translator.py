import re
from dataclasses import dataclass
import dagster as dg

@dataclass
class SQLMeshTranslator:
    def normalize_segment(self, segment: str) -> str:
        # Enlève les guillemets, espaces, et remplace tout caractère non valide par un underscore
        segment = segment.replace('"', '').replace("'", "")
        return re.sub(r'[^A-Za-z0-9_]', '_', segment)

    def get_asset_key(self, model) -> dg.AssetKey:
        if hasattr(model, "fqn"):
            parts = [self.normalize_segment(s) for s in model.fqn.split(".")]
        else:
            parts = [self.normalize_segment(model.name)]
        return dg.AssetKey(parts)

    def get_asset_key_from_fqn(self, fqn: str) -> dg.AssetKey:
        return dg.AssetKey([self.normalize_segment(s) for s in fqn.split(".")])

    def get_deps_from_fqn(self, model_fqn: str, dag_graph: dict) -> list:
        deps_fqn = dag_graph.get(model_fqn, set())
        return [self.get_asset_key_from_fqn(dep) for dep in deps_fqn] 