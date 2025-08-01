from dagster import AssetSpec, AssetCheckSpec
from sqlmesh.core.model.definition import ExternalModel
from sqlmesh.utils.date import now
from typing import Any
from datetime import datetime
from .sqlmesh_asset_check_utils import create_all_asset_checks

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


def get_model_partitions_from_plan(plan, translator, asset_key, snapshot) -> dict:
    """Retourne les informations de partition pour un asset en utilisant le plan."""
    # Convertir AssetKey vers le modèle SQLMesh
    model = snapshot.model if snapshot else None
    
    if model:
        partitioned_by = getattr(model, "partitioned_by", [])
        # Extraire les noms des colonnes de partition
        partition_columns = [col.name for col in partitioned_by] if partitioned_by else []
        
        # Utiliser les intervals du snapshot du plan (qui est catégorisé)
        intervals = getattr(snapshot, "intervals", [])
        grain = getattr(model, "grain", [])
        is_partitioned = len(partition_columns) > 0
        
        return {
            "partitioned_by": partition_columns, 
            "intervals": intervals, 
            "partition_columns": partition_columns, 
            "grain": grain, 
            "is_partitioned": is_partitioned
        }
    
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


def has_breaking_changes_with_message(plan, logger, context=None) -> tuple[bool, str]:
    """
    Returns (True, message) if the given SQLMesh plan contains breaking changes
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
        return True, msg
    else:
        info_msg = f"No breaking changes detected in plan {getattr(plan, 'plan_id', None)}."
        if context and hasattr(context, "log"):
            context.log.info(info_msg)
        else:
            logger.info(info_msg)
        return False, info_msg


def get_asset_kinds(sqlmesh_resource) -> set:
    """
    Retourne les kinds des assets avec le dialecte SQL.
    """
    translator = sqlmesh_resource.translator
    context = sqlmesh_resource.context
    dialect = translator._get_context_dialect(context)
    return {"sqlmesh", dialect}


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


def format_partition_metadata(model_partitions: dict) -> dict:
    """
    Formate les métadonnées de partition pour les rendre plus lisibles.
    
    Args:
        model_partitions: Dict avec les infos de partition brutes de SQLMesh
    
    Returns:
        Dict avec les métadonnées formatées
    """
    formatted_metadata = {}
    
    # Colonnes de partition (on prend partitioned_by qui est plus standard)
    if model_partitions.get("partitioned_by"):
        formatted_metadata["partition_columns"] = model_partitions["partitioned_by"]
    
    # Intervalles convertis en datetime lisible
    if model_partitions.get("intervals"):
        readable_intervals = []
        intervals = model_partitions["intervals"]
        
        for interval in intervals:
            if len(interval) == 2:
                start_ts, end_ts = interval
                # Convertir les timestamps Unix (millisecondes) en datetime
                start_dt = datetime.fromtimestamp(start_ts / 1000).strftime("%Y-%m-%d %H:%M:%S")
                end_dt = datetime.fromtimestamp(end_ts / 1000).strftime("%Y-%m-%d %H:%M:%S")
                readable_intervals.append({
                    "start": start_dt,
                    "end": end_dt,
                    "start_timestamp": start_ts,
                    "end_timestamp": end_ts
                })
        
        # Utiliser directement l'objet Python (Dagster peut le gérer)
        formatted_metadata["partition_intervals"] = readable_intervals
    
    # Grain (si présent et non vide)
    if model_partitions.get("grain") and model_partitions["grain"]:
        formatted_metadata["partition_grain"] = model_partitions["grain"]
    
    return formatted_metadata


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


def safe_extract_audit_query(model, audit_obj, audit_args, logger=None):
    """
    Extrait la query d'audit de manière sécurisée avec fallback.
    
    Args:
        model: Modèle SQLMesh
        audit_obj: Objet d'audit SQLMesh
        audit_args: Arguments de l'audit
        logger: Logger optionnel pour les warnings
    
    Returns:
        str: La query SQL ou "N/A" si extraction échoue
    """
    try:
        return model.render_audit_query(audit_obj, **audit_args).sql()
    except Exception as e:
        if logger:
            logger.warning(f"⚠️ Erreur lors du rendu de la query d'audit: {e}")
        try:
            return audit_obj.query.sql()
        except Exception as e2:
            if logger:
                logger.warning(f"⚠️ Erreur lors de l'extraction de la query de base: {e2}")
            return "N/A"


def analyze_sqlmesh_crons_using_api(context):
    """
    Analyse tous les crons des modèles SQLMesh et retourne le schedule Dagster recommandé.
    
    Args:
        context: SQLMesh Context
    
    Returns:
        str: Expression cron Dagster recommandée
    """
    try:
        models = context.models.values()
        
        # Collecter les intervalles des modèles avec cron
        intervals = []
        for model in models:
            if hasattr(model, 'cron') and model.cron:
                intervals.append(model.interval_unit.seconds)
        
        if not intervals:
            return "0 */6 * * *"  # Default: toutes les 6h
        
        # Trouver la granularité la plus fine
        finest_interval = min(intervals)
        
        # Retourner le schedule Dagster recommandé
        return get_dagster_schedule_from_interval(finest_interval)
        
    except Exception as e:
        # Fallback en cas d'erreur
        return "0 */6 * * *"  # Default: toutes les 6h


def get_dagster_schedule_from_interval(interval_seconds):
    """
    Convertit un intervalle en secondes vers une expression cron Dagster.
    
    Args:
        interval_seconds: Intervalle en secondes
    
    Returns:
        str: Expression cron Dagster
    """
    # Mapping des intervalles vers les expressions cron
    if interval_seconds <= 300:  # <= 5 minutes
        return "*/5 * * * *"
    elif interval_seconds <= 900:  # <= 15 minutes
        return "*/15 * * * *"
    elif interval_seconds <= 1800:  # <= 30 minutes
        return "*/30 * * * *"
    elif interval_seconds <= 3600:  # <= 1 heure
        return "0 * * * *"
    elif interval_seconds <= 21600:  # <= 6 heures
        return "0 */6 * * *"
    elif interval_seconds <= 86400:  # <= 1 jour
        return "0 0 * * *"
    else:
        return "0 0 * * 0"  # Toutes les semaines




def validate_external_dependencies(sqlmesh_resource, models) -> list:
    """
    Valide que tous les external dependencies peuvent être proprement mappés.
    Retourne une liste d'erreurs de validation.
    """
    translator = sqlmesh_resource.translator
    context = sqlmesh_resource.context
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

def create_all_asset_specs(
    models,
    sqlmesh_resource,
    extra_keys,
    kinds,
    owners,
    group_name
) -> list[AssetSpec]:
    """
    Crée tous les AssetSpec pour tous les modèles SQLMesh.
    
    Args:
        models: Liste des modèles SQLMesh
        sqlmesh_resource: SQLMeshResource
        extra_keys: Clés supplémentaires pour les métadonnées
        kinds: Kinds des assets
        owners: Propriétaires des assets
        group_name: Nom du groupe par défaut
    
    Returns:
        Liste de tous les AssetSpec
    """
    translator = sqlmesh_resource.translator
    context = sqlmesh_resource.context
    specs = []
    for model in models:
        asset_key = translator.get_asset_key(model)
        code_version = str(getattr(model, "data_hash", "")) if hasattr(model, "data_hash") and getattr(model, "data_hash") else None
        metadata = get_asset_metadata(translator, model, code_version, extra_keys, owners)
        tags = get_asset_tags(translator, context, model)
        deps = translator.get_model_deps_with_external(context, model)
        final_group_name = translator.get_group_name_with_fallback(context, model, group_name)
        
        spec = AssetSpec(
            key=asset_key,
            deps=deps,
            code_version=code_version,
            metadata=metadata,
            kinds=kinds,
            tags=tags,
            group_name=final_group_name,
        )
        specs.append(spec)
    return specs


def create_asset_specs(
    sqlmesh_resource,
    extra_keys,
    kinds,
    owners,
    group_name
) -> list[AssetSpec]:
    """
    Crée tous les AssetSpec pour tous les modèles SQLMesh.
    
    Args:
        sqlmesh_resource: SQLMeshResource
        extra_keys: Clés supplémentaires pour les métadonnées
        kinds: Kinds des assets
        owners: Propriétaires des assets
        group_name: Nom du groupe par défaut
    
    Returns:
        Liste de tous les AssetSpec
    """
    models = [model for model in sqlmesh_resource.get_models() if not isinstance(model, ExternalModel)]
    return create_all_asset_specs(models, sqlmesh_resource, extra_keys, kinds, owners, group_name)


def get_extra_keys() -> list[str]:
    """
    Retourne les clés supplémentaires pour les métadonnées des assets SQLMesh.
    
    Returns:
        Liste des clés supplémentaires
    """
    return ["cron", "tags", "kind", "dialect", "query", "partitioned_by", "clustered_by"]


def create_asset_checks(
    sqlmesh_resource
) -> list[AssetCheckSpec]:
    """
    Crée tous les AssetCheckSpec pour tous les modèles SQLMesh.
    
    Args:
        sqlmesh_resource: SQLMeshResource
    
    Returns:
        Liste de tous les AssetCheckSpec
    """
    models = [model for model in sqlmesh_resource.get_models() if not isinstance(model, ExternalModel)]
    return create_all_asset_checks(models, sqlmesh_resource.translator) 