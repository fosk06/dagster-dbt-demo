import re
from dataclasses import dataclass
from dagster import AssetKey
from dagster._core.definitions.metadata import TableMetadataSet, TableSchema, TableColumn
import json
from typing import Optional
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

    def get_group_name_with_fallback(self, context, model, factory_group_name: str) -> str:
        """
        Détermine le group_name avec fallback vers la factory.
        Priorité : tag > factory > fallback par défaut
        """
        # Vérifier les tags SQLMesh pour les propriétés Dagster
        dagster_property = self._get_dagster_property_from_tags(model, "group_name")
        if dagster_property:
            return dagster_property
        
        # Si pas de tag, utiliser la valeur de la factory
        if factory_group_name:
            return factory_group_name
        
        # Fallback: logique par défaut
        path = self.get_asset_key_name(getattr(model, "fqn", getattr(model, "view_name", "")))
        return path[-2] if len(path) >= 2 else "default"

    def _get_dagster_property_from_tags(self, model, property_name: str) -> Optional[str]:
        """
        Parse les tags SQLMesh pour extraire les propriétés Dagster.
        Convention: "dagster:property_name:value"
        """
        tags = getattr(model, "tags", set())
        
        for tag in tags:
            if tag.startswith("dagster:"):
                parts = tag.split(":")
                if len(parts) >= 3 and parts[1] == property_name:
                    return parts[2]
        
        return None

    def get_tags(self, context, model) -> dict:
        """Retourne les tags du modèle sous forme de dict."""
        tags = getattr(model, "tags", set())
        
        # Filtrer les tags de configuration Dagster
        dagster_tags = {}
        for tag in tags:
            # Ignorer les tags qui commencent par "dagster:" (configuration interne)
            if not tag.startswith("dagster:"):
                dagster_tags[tag] = "true"
        
        return dagster_tags

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