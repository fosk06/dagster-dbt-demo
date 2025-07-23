import re
from dataclasses import dataclass
import dagster as dg

@dataclass
class SQLMeshTranslator:
    def normalize_segment(self, segment: str) -> str:
        segment = segment.replace('"', '').replace("'", "")
        return re.sub(r'[^A-Za-z0-9_]', '_', segment)

    def get_asset_key(self, model) -> dg.AssetKey:
        catalog = self.normalize_segment(getattr(model, "catalog", "default"))
        schema = self.normalize_segment(getattr(model, "schema_name", "default"))
        view = self.normalize_segment(getattr(model, "view_name", "unknown"))
        return dg.AssetKey([catalog, schema, view])

    def get_asset_key_from_dep_str(self, dep_str: str) -> dg.AssetKey:
        # Parse une string du type '"catalog"."schema"."view"'
        parts = [self.normalize_segment(s) for s in re.findall(r'"([^"]+)"', dep_str)]
        if len(parts) == 3:
            return dg.AssetKey(parts)
        # Fallback: split sur les points si pas de guillemets
        return dg.AssetKey([self.normalize_segment(s) for s in dep_str.split(".")])

    def get_deps_from_model(self, model) -> list:
        # model.depends_on est un set de strings du type '"catalog"."schema"."view"'
        depends_on = getattr(model, "depends_on", set())
        return [self.get_asset_key_from_dep_str(dep) for dep in depends_on] 