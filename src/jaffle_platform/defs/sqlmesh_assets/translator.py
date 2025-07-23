from dataclasses import dataclass
import dagster as dg

@dataclass
class SQLMeshTranslator:
    def get_asset_key(self, model) -> dg.AssetKey:
        # Adapt this if your model object has a different attribute for the name
        return dg.AssetKey(model.name) 