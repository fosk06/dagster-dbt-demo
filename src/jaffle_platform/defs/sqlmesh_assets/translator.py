import re
from dataclasses import dataclass
import dagster as dg
from dagster._core.definitions.metadata import TableMetadataSet, TableSchema, TableColumn
import json

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

    def get_table_metadata(self, model) -> TableMetadataSet:
        columns_to_types = getattr(model, "columns_to_types", {})
        columns = [
            TableColumn(
                name=col,
                type=str(getattr(dtype, "this", dtype)),  # conversion explicite en str
                description=None  # Ajoute une description si disponible
            )
            for col, dtype in columns_to_types.items()
        ]
        table_schema = TableSchema(columns=columns)
        table_name = ".".join([
            getattr(model, "catalog", "default"),
            getattr(model, "schema_name", "default"),
            getattr(model, "view_name", "unknown"),
        ])
        return TableMetadataSet(
            column_schema=table_schema,
            table_name=table_name,
        )

    def serialize_metadata(self, model, keys: list[str]) -> dict:
        """
        Serialize the model as JSON and extract only the specified keys.
        Returns a dict {key: value} for each key found in the model's JSON.
        """
        model_metadata = json.loads(model.json()) if hasattr(model, "json") else {}
        return {f"dagster-sqlmesh/{key}": model_metadata.get(key) for key in keys}

    def get_assetkey_to_model(self, models: list) -> dict:
        """
        Returns a mapping {AssetKey: model} for a given list of SQLMesh models.
        """
        return {self.get_asset_key(model): model for model in models}