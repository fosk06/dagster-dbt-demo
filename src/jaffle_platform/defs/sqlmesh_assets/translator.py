import re
from dataclasses import dataclass, field
from dagster import AssetKey, PartitionsDefinition
from dagster._core.definitions.metadata import TableMetadataSet, TableSchema, TableColumn
from dagster import (
    DailyPartitionsDefinition,
    WeeklyPartitionsDefinition, 
    MonthlyPartitionsDefinition,
    TimeWindowPartitionsDefinition
)
from dagster._core.definitions.partitions.schedule_type import ScheduleType
import json
from typing import Callable, Optional, Mapping, Any
from datetime import datetime
from sqlmesh.core.model.definition import ExternalModel

@dataclass
class SQLMeshTranslator:
    """
    Translator pour mapper les concepts SQLMesh vers Dagster.
    Suit le pattern dagster-dbt avec des méthodes extensibles.
    """
    
    def normalize_segment(self, segment: str) -> str:
        """Normalise un segment d'AssetKey en remplaçant les caractères spéciaux."""
        segment = segment.replace('"', '').replace("'", "")
        return re.sub(r'[^A-Za-z0-9_]', '_', segment)

    def get_asset_key(self, model) -> AssetKey:
        """
        Génère un AssetKey pour un modèle SQLMesh.
        Peut être override pour un mapping custom.
        """
        catalog = self.normalize_segment(getattr(model, "catalog", "default"))
        schema = self.normalize_segment(getattr(model, "schema_name", "default"))
        view = self.normalize_segment(getattr(model, "view_name", "unknown"))
        return AssetKey([catalog, schema, view])

    def get_external_asset_key(self, external_fqn: str) -> AssetKey:
        """
        Génère un AssetKey pour un asset externe (source SQLMesh).
        Peut être override pour un mapping custom.
        """
        # Parse une string du type '"catalog"."schema"."view"'
        parts = [self.normalize_segment(s) for s in re.findall(r'"([^"]+)"', external_fqn)]
        if len(parts) == 3:
            catalog, schema, table = parts
            # Mapping par défaut pour les sources externes
            if catalog == "main" and schema == "external":
                return AssetKey(["sling", table])
            elif catalog == "jaffle_db" and schema == "external":
                return AssetKey(["sling", table])
            else:
                # Fallback: use the original structure but with "external" prefix
                return AssetKey(["external", catalog, schema, table])

        # Fallback for non-quoted strings
        parts = [self.normalize_segment(s) for s in external_fqn.split(".")]
        return AssetKey(["external"] + parts)

    def get_asset_key_from_dep_str(self, dep_str: str) -> AssetKey:
        """Parse une string de dépendance et retourne un AssetKey."""
        parts = [self.normalize_segment(s) for s in re.findall(r'"([^"]+)"', dep_str)]
        if len(parts) == 3:
            return AssetKey(parts)
        # Fallback: split sur les points si pas de guillemets
        return AssetKey([self.normalize_segment(s) for s in dep_str.split(".")])

    def get_deps_from_model(self, model) -> list:
        """Retourne les dépendances d'un modèle (version simple, sans external assets)."""
        depends_on = getattr(model, "depends_on", set())
        return [self.get_asset_key_from_dep_str(dep) for dep in depends_on]

    def get_model_deps_with_external(self, context, model) -> list:
        """
        Retourne les dépendances d'un modèle, distinguant les modèles internes SQLMesh
        et les assets externes (comme Sling assets).
        Peut être override pour un mapping custom des external assets.
        """
        depends_on = getattr(model, "depends_on", set())
        deps = []

        for dep_str in depends_on:
            dep_asset_key = self.get_asset_key_from_dep_str(dep_str)

            # Check if this dependency is an internal SQLMesh model
            dep_model = context.get_model(dep_str)

            # Check if this is an ExternalModel
            if dep_model and not isinstance(dep_model, ExternalModel):
                # Internal SQLMesh model
                deps.append(dep_asset_key)
            else:
                # External asset (like Sling) - utilise le mapping custom
                external_asset_key = self.get_external_asset_key(dep_str)
                deps.append(external_asset_key)

        return deps

    def get_table_metadata(self, model) -> TableMetadataSet:
        """Génère les métadonnées de table pour un modèle."""
        columns_to_types = getattr(model, "columns_to_types", {})
        
        # Récupérer les descriptions de colonnes
        column_descriptions = getattr(model, "column_descriptions", {})
        
        columns = [
            TableColumn(
                name=col,
                type=str(getattr(dtype, "this", dtype)),
                description=column_descriptions.get(col)  # Utiliser la description si disponible
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
        """Sérialise les métadonnées du modèle en JSON."""
        model_metadata = json.loads(model.json()) if hasattr(model, "json") else {}
        return {f"dagster-sqlmesh/{key}": model_metadata.get(key) for key in keys}

    def get_assetkey_to_model(self, models: list) -> dict:
        """Retourne un mapping {AssetKey: model} pour une liste de modèles SQLMesh."""
        return {self.get_asset_key(model): model for model in models}

    def get_asset_key_name(self, fqn: str) -> list:
        """Découpe un FQN en segments (catalog, schema, name)."""
        return [self.normalize_segment(s) for s in fqn.split(".")]

    def get_group_name(self, context, model) -> str:
        """
        Détermine le group_name pour un modèle.
        Peut être override pour un mapping custom.
        """
        path = self.get_asset_key_name(getattr(model, "fqn", getattr(model, "view_name", "")))
        return path[-2] if len(path) >= 2 else "default"

    def get_tags(self, context, model) -> dict:
        """Retourne les tags du modèle sous forme de dict."""
        tags = getattr(model, "tags", set())
        return {k: "true" for k in tags}

    def _get_context_dialect(self, context) -> str:
        """Retourne le dialecte SQL du contexte SQLMesh."""
        return getattr(getattr(context, "engine_adapter", None), "dialect", "")

    # --- Méthodes utilitaires pour les external assets ---

    def is_external_dependency(self, context, dep_str: str) -> bool:
        """Vérifie si une dépendance fait référence à un asset externe."""
        return context.get_model(dep_str) is None

    def get_external_dependencies(self, context, model) -> list:
        """Retourne seulement les dépendances externes d'un modèle."""
        depends_on = getattr(model, "depends_on", set())
        external_deps = []

        for dep_str in depends_on:
            if self.is_external_dependency(context, dep_str):
                external_deps.append(dep_str)

        return external_deps

    def get_internal_dependencies(self, context, model) -> list:
        """Retourne seulement les dépendances internes SQLMesh d'un modèle."""
        depends_on = getattr(model, "depends_on", set())
        internal_deps = []

        for dep_str in depends_on:
            if not self.is_external_dependency(context, dep_str):
                internal_deps.append(dep_str)

        return internal_deps

    def map_sqlmesh_cron_to_dagster_partitions(self, cron_expression: str, start_date: str = "2024-01-01") -> Optional[PartitionsDefinition]:
        """Convertit les types de partitions SQLMesh vers Dagster"""
        
        # Mapping des expressions cron SQLMesh vers les types de schedule Dagster
        schedule_mapping = {
            "@hourly": ScheduleType.HOURLY,
            "@daily": ScheduleType.DAILY,
            "@weekly": ScheduleType.WEEKLY,
            "@monthly": ScheduleType.MONTHLY,
        }
        
        schedule_type = schedule_mapping.get(cron_expression)
        
        if schedule_type:
            # Déterminer le format selon le type de schedule
            if schedule_type == ScheduleType.HOURLY:
                fmt = "%Y-%m-%d-%H"
            else:
                fmt = "%Y-%m-%d"
            
            return TimeWindowPartitionsDefinition(
                start=datetime.fromisoformat(start_date),
                end=datetime.now(),
                fmt=fmt,
                schedule_type=schedule_type
            )
        
        return None

    def get_partitions_def(self, model) -> Optional[PartitionsDefinition]:
        """Détermine les partitions Dagster à partir des propriétés SQLMesh"""
        
        # Récupérer les infos de partition SQLMesh
        partitioned_by = getattr(model, "partitioned_by", [])
        cron = getattr(model, "cron", None)
        
        if not partitioned_by or not cron:
            return None
            
        # Mapper vers Dagster
        return self.map_sqlmesh_cron_to_dagster_partitions(cron)

    def get_sqlmesh_partition_info(self, snapshot) -> dict:
        """Extrait les infos de partition d'un snapshot SQLMesh"""
        
        return {
            "partitioned_by": getattr(snapshot, "partitioned_by", []),
            "cron": getattr(snapshot, "cron", None),
            "intervals": getattr(snapshot, "intervals", []),
            "partition_columns": getattr(snapshot, "partition_columns", [])
        }


class CustomSQLMeshTranslator(SQLMeshTranslator):
    """
    Exemple de translator custom pour montrer comment étendre le mapping.
    """
    
    def get_external_asset_key(self, external_fqn: str) -> AssetKey:
        """
        Override pour un mapping custom des external assets.
        Exemple: 'jaffle_db.main.raw_source_customers' → ['target', 'main', 'raw_source_customers']
        """
        parts = external_fqn.replace('"', '').split('.')
        # On ignore le catalog (jaffle_db), on prend le reste
        return AssetKey(['target'] + parts[1:])

    def get_group_name(self, context, model) -> str:
        """
        Override pour des groupes custom.
        """
        # Exemple: les modèles staging dans le groupe "staging", les marts dans "marts"
        model_name = getattr(model, "view_name", "")
        if model_name.startswith("stg_"):
            return "staging"
        elif model_name.startswith("mart_"):
            return "marts"
        return super().get_group_name(context, model)