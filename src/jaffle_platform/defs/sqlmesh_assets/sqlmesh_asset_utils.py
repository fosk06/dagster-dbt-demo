# Utility functions for SQLMeshResource and Dagster integration

from sqlmesh.core.model.definition import ExternalModel
from typing import List, Dict, Any, Optional


def extract_metadata(obj, fields: list[str], prefix: str = "sqlmesh_") -> dict:
    """
    Extract and format the specified fields from a SQLMesh object (plan, model, etc.)
    for use as Dagster asset metadata.
    """
    return {f"{prefix}{field}": str(getattr(obj, field, None)) for field in fields}


def extract_plan_metadata(plan) -> dict:
    """
    Extracts and formats a standard set of metadata fields from a SQLMesh plan object
    for use as Dagster AssetMaterialization metadata.
    """
    fields = [
        "plan_id", "environment", "start", "end", "has_changes",
        "models_to_backfill", "requires_backfill", "modified_snapshots", "user_provided_flags"
    ]
    return extract_metadata(plan, fields, prefix="sqlmesh_plan_")


def get_models_to_materialize(selected_asset_keys, get_models_func, translator):
    """
    Retourne les modèles SQLMesh à matérialiser, en excluant les external models.
    """
    all_models = get_models_func()
    
    # Filtrer les external models
    internal_models = []
    for model in all_models:
        # Vérifier si c'est un ExternalModel
        if not isinstance(model, ExternalModel):
            internal_models.append(model)
    
    # Si des assets spécifiques sont sélectionnés, filtrer par AssetKey
    if selected_asset_keys:
        assetkey_to_model = translator.get_assetkey_to_model(internal_models)
        models_to_materialize = []
        
        for asset_key in selected_asset_keys:
            if asset_key in assetkey_to_model:
                models_to_materialize.append(assetkey_to_model[asset_key])
        
        return models_to_materialize
    
    # Sinon, retourner tous les modèles internes
    return internal_models


def get_assetkey_to_snapshot(context, translator) -> dict:
    """
    Returns a mapping {AssetKey: snapshot} for all models in the current context.
    context: SQLMesh Context
    translator: SQLMeshTranslator instance
    """
    assetkey_to_snapshot = {}
    for snapshot in context.snapshots.values():
        model = snapshot.model
        asset_key = translator.get_asset_key(model)
        assetkey_to_snapshot[asset_key] = snapshot
    return assetkey_to_snapshot

def get_model_partitions(context, translator, asset_key, snapshot) -> dict:
    """Retourne les informations de partition pour un asset."""
    # Convertir AssetKey vers le modèle SQLMesh
    model = get_model_from_asset_key(context, translator, asset_key)
    
    if model:
        partitioned_by = getattr(model, "partitioned_by", [])
        # Extraire les noms des colonnes de partition
        partition_columns = [col.name for col in partitioned_by] if partitioned_by else []
        intervals = getattr(snapshot, "intervals", [])
        grain = getattr(model, "grain", [])
        is_partitioned = len(partition_columns) > 0
        return {"partitioned_by": partition_columns, "intervals": intervals, "partition_columns": partition_columns, "grain": grain, "is_partitioned": is_partitioned}
    
    return {"partitioned_by": [], "intervals": []}


def get_model_from_asset_key(context, translator, asset_key) -> Any:
    """Convertit un AssetKey Dagster vers le modèle SQLMesh correspondant."""
    # Utiliser le mapping inverse du translator
    all_models = list(context.models.values())
    assetkey_to_model = translator.get_assetkey_to_model(all_models)
    
    return assetkey_to_model.get(asset_key)

def get_topologically_sorted_asset_keys(context, translator, selected_asset_keys) -> list:
    """
    Returns the selected_asset_keys sorted in topological order according to the SQLMesh DAG.
    context: SQLMesh Context
    translator: SQLMeshTranslator instance
    """
    models = list(context.models.values())
    assetkey_to_model = translator.get_assetkey_to_model(models)
    fqn_to_model = {model.fqn: model for model in models}
    fqn_to_assetkey = {model.fqn: translator.get_asset_key(model) for model in models}
    selected_fqns = set(model.fqn for key, model in assetkey_to_model.items() if key in selected_asset_keys)
    topo_fqns = context.dag.sorted
    ordered_asset_keys = [
        fqn_to_assetkey[fqn]
        for fqn in topo_fqns
        if fqn in selected_fqns and fqn in fqn_to_assetkey
    ]
    return ordered_asset_keys


def has_breaking_changes(plan, logger, context=None) -> bool:
    """
    Returns True if the given SQLMesh plan contains breaking changes
    (any directly or indirectly modified models).
    Logs the models concernés, using context.log if available.
    """
    directly_modified = getattr(plan, "directly_modified", set())
    indirectly_modified = getattr(plan, "indirectly_modified", set())

    directly = list(directly_modified)
    indirectly = [item for sublist in indirectly_modified.values() for item in sublist]

    has_changes = bool(directly or indirectly)

    if has_changes:
        msg = (
            f"Breaking changes detected in plan {getattr(plan, 'plan_id', None)}! "
            f"Directly modified models: {directly} | Indirectly modified models: {indirectly}"
        )
        if context and hasattr(context, "log"):
            context.log.error(msg)
        else:
            logger.error(msg)
    else:
        info_msg = f"No breaking changes detected in plan {getattr(plan, 'plan_id', None)}."
        if context and hasattr(context, "log"):
            context.log.info(info_msg)
        else:
            logger.info(info_msg)

    return has_changes 


def get_asset_kinds(translator, context) -> dict:
    """
    Retourne les kinds des assets.
    """
    return {"sqlmesh": "sqlmesh"}


def get_asset_group_name(translator, context, model) -> str:
    """
    Retourne le group_name pour un asset.
    """
    return translator.get_group_name(context, model)


def get_asset_tags(translator, context, model) -> dict:
    """
    Retourne les tags pour un asset.
    """
    return translator.get_tags(context, model)


def get_asset_metadata(translator, model, code_version, extra_keys, owners) -> dict:
    """
    Retourne les métadonnées pour un asset.
    """
    metadata = {}
    
    # Métadonnées de base
    if code_version:
        metadata["code_version"] = code_version
    
    # Métadonnées de table avec column descriptions
    table_metadata = translator.get_table_metadata(model)
    metadata.update(table_metadata)
    
    # Ajouter les column descriptions si disponibles
    column_descriptions = get_column_descriptions_from_model(model)
    if column_descriptions:
        metadata["column_descriptions"] = column_descriptions
    
    # Métadonnées supplémentaires
    if extra_keys:
        serialized_metadata = translator.serialize_metadata(model, extra_keys)
        metadata.update(serialized_metadata)
    
    # Propriétaires
    if owners:
        metadata["owners"] = owners
    
    return metadata


def get_column_descriptions_from_model(model) -> dict:
    """
    Extrait les column_descriptions d'un modèle SQLMesh et les formate pour Dagster.
    """
    column_descriptions = {}
    
    # Essayer d'accéder aux column_descriptions du modèle
    if hasattr(model, 'column_descriptions') and model.column_descriptions:
        column_descriptions = model.column_descriptions
    
    # Essayer d'accéder via le modèle SQLMesh
    elif hasattr(model, 'model') and hasattr(model.model, 'column_descriptions'):
        column_descriptions = model.model.column_descriptions
    
    return column_descriptions


# --- Nouvelles fonctions pour gérer les external assets ---

def get_external_dependencies_for_models(context, translator, models) -> dict:
    """
    Retourne un mapping de model FQN vers liste d'external dependencies.
    context: SQLMesh Context
    translator: SQLMeshTranslator instance
    models: list de SQLMesh models
    """
    external_deps = {}
    for model in models:
        # Ignorer les external models
        if isinstance(model, ExternalModel):
            continue
            
        external_deps[model.fqn] = translator.get_external_dependencies(context, model)
    return external_deps


def get_internal_dependencies_for_models(context, translator, models) -> dict:
    """
    Retourne un mapping de model FQN vers liste d'internal dependencies.
    context: SQLMesh Context
    translator: SQLMeshTranslator instance
    models: list de SQLMesh models
    """
    internal_deps = {}
    for model in models:
        # Ignorer les external models
        if isinstance(model, ExternalModel):
            continue
            
        internal_deps[model.fqn] = translator.get_internal_dependencies(context, model)
    return internal_deps


def validate_external_dependencies(context, translator, models) -> list:
    """
    Valide que tous les external dependencies peuvent être proprement mappés.
    Retourne une liste d'erreurs de validation.
    """
    errors = []
    for model in models:
        # Ignorer les external models dans la validation
        if isinstance(model, ExternalModel):
            continue
            
        external_deps = translator.get_external_dependencies(context, model)
        for dep_str in external_deps:
            try:
                translator.get_external_asset_key(dep_str)
            except Exception as e:
                errors.append(f"Failed to map external dependency '{dep_str}' for model '{model.name}': {e}")
    return errors


def get_all_external_asset_keys(context, translator, models) -> set:
    """
    Retourne tous les external asset keys qui sont référencés par les modèles donnés.
    """
    external_keys = set()
    for model in models:
        # Ignorer les external models
        if isinstance(model, ExternalModel):
            continue
            
        external_deps = translator.get_external_dependencies(context, model)
        for dep_str in external_deps:
            asset_key = translator.get_external_asset_key(dep_str)
            external_keys.add(asset_key)
    return external_keys 