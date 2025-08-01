from dagster import (
    multi_asset,
    AssetExecutionContext,
    RetryPolicy,
    schedule,
    define_asset_job,
    RunRequest,
    Definitions,
    op,
    job,
)
from .resource import SQLMeshResource
from .sqlmesh_asset_utils import (
    get_asset_kinds,
    get_extra_keys,
    create_asset_specs,
    create_asset_checks,
    validate_external_dependencies,
)
import datetime
from .translator import SQLMeshTranslator
from typing import Optional, Dict, Set, List, Any

def sqlmesh_assets_factory(
    *,
    sqlmesh_resource: SQLMeshResource,
    name: str = "sqlmesh_assets",
    group_name: str = "sqlmesh",
    op_tags: Optional[Dict[str, Any]] = None,
    required_resource_keys: Optional[Set[str]] = None,
    retry_policy: Optional[RetryPolicy] = None,
    owners: Optional[List[str]] = None,
):
    """
    Factory pour créer des assets SQLMesh Dagster.
    
    Args:
        sqlmesh_resource: La resource SQLMesh configurée
        name: Nom du multi_asset
        group_name: Groupe par défaut pour les assets
        op_tags: Tags pour l'opération
        required_resource_keys: Clés de resources requises
        retry_policy: Politique de retry
        owners: Propriétaires des assets
    """
    try:
        extra_keys = get_extra_keys()
        kinds = get_asset_kinds(sqlmesh_resource)

        # Créer les AssetSpec et AssetCheckSpec
        specs = create_asset_specs(sqlmesh_resource, extra_keys, kinds, owners, group_name)
        asset_checks = create_asset_checks(sqlmesh_resource)
    except Exception as e:
        raise ValueError(f"Failed to create SQLMesh assets: {e}") from e

    @multi_asset(
        name=name,
        specs=specs,
        check_specs=asset_checks,
        op_tags=op_tags,
        retry_policy=retry_policy,
        can_subset=True
    )
    def _sqlmesh_assets(context: AssetExecutionContext, sqlmesh: SQLMeshResource):
        context.log.info("🚀 Starting SQLMesh materialization")
        
        try:
            yield from sqlmesh.materialize_all_assets(context)
            context.log.info("✅ SQLMesh materialization completed")
        except Exception as e:
            context.log.error(f"❌ SQLMesh materialization failed: {e}")
            raise

    return _sqlmesh_assets


def sqlmesh_adaptive_schedule_factory(
    *,
    sqlmesh_resource: SQLMeshResource,
    name: str = "sqlmesh_adaptive_schedule",
):
    """
    Factory pour créer un schedule Dagster adaptatif basé sur les crons SQLMesh.
    
    Args:
        sqlmesh_resource: La resource SQLMesh configurée
        name: Nom du schedule
    """
    
    # Obtenir le schedule recommandé basé sur les crons SQLMesh
    recommended_schedule = sqlmesh_resource.get_recommended_schedule()
    
    # Créer automatiquement le job SQLMesh avec multi_asset (pour AssetCheckResult)
    sqlmesh_assets = sqlmesh_assets_factory(sqlmesh_resource=sqlmesh_resource)
    sqlmesh_job = define_asset_job(
        name="sqlmesh_job",
        selection=[sqlmesh_assets],
    )
    
    @schedule(
        job=sqlmesh_job,
        cron_schedule=recommended_schedule,
        name=name,
        description=f"Schedule adaptatif basé sur les crons SQLMesh (granularité: {recommended_schedule})"
    )
    def _sqlmesh_adaptive_schedule(context):
        return RunRequest(
            run_key=f"sqlmesh_adaptive_{datetime.datetime.now().isoformat()}",
            tags={"schedule": "sqlmesh_adaptive", "granularity": recommended_schedule}
        )
    
    return _sqlmesh_adaptive_schedule, sqlmesh_job, sqlmesh_assets


def sqlmesh_definitions_factory(
    *,
    project_dir: str = "sqlmesh_project",
    gateway: str = "postgres",
    concurrency_limit: int = 1,
    ignore_cron: bool = False,
    translator: Optional[SQLMeshTranslator] = None,
    name: str = "sqlmesh_assets",
    group_name: str = "sqlmesh",
    op_tags: Optional[Dict[str, Any]] = None,
    required_resource_keys: Optional[Set[str]] = None,
    retry_policy: Optional[RetryPolicy] = None,
    owners: Optional[List[str]] = None,
    schedule_name: str = "sqlmesh_adaptive_schedule",
):
    """
    Factory tout-en-un pour créer une intégration SQLMesh complète avec Dagster.
    
    Args:
        project_dir: Répertoire du projet SQLMesh
        gateway: Gateway SQLMesh (postgres, duckdb, etc.)
        concurrency_limit: Limite de concurrence
        ignore_cron: Ignorer les crons (pour les tests)
        translator: Translator custom pour les asset keys
        name: Nom du multi_asset
        group_name: Groupe par défaut pour les assets
        op_tags: Tags pour l'opération
        required_resource_keys: Clés de resources requises
        retry_policy: Politique de retry
        owners: Propriétaires des assets
        schedule_name: Nom du schedule adaptatif
    """
    
    # Validation des paramètres
    if concurrency_limit < 1:
        raise ValueError("concurrency_limit must be >= 1")
    
    # Valeurs par défaut robustes
    op_tags = op_tags or {"sqlmesh": "true"}
    required_resource_keys = required_resource_keys or {"sqlmesh"}
    owners = owners or []
    
    # Créer la resource SQLMesh
    sqlmesh_resource = SQLMeshResource(
        project_dir=project_dir,
        gateway=gateway,
        translator=translator,
        concurrency_limit=concurrency_limit,
        ignore_cron=ignore_cron
    )
    
    # Valider les external dependencies
    try:
        models = sqlmesh_resource.get_models()
        validation_errors = validate_external_dependencies(sqlmesh_resource, models)
        if validation_errors:
            raise ValueError(f"External dependencies validation failed:\n" + "\n".join(validation_errors))
    except Exception as e:
        raise ValueError(f"Failed to validate external dependencies: {e}") from e
    
    # Créer les assets SQLMesh
    sqlmesh_assets = sqlmesh_assets_factory(
        sqlmesh_resource=sqlmesh_resource,
        name=name,
        group_name=group_name,
        op_tags=op_tags,
        required_resource_keys=required_resource_keys,
        retry_policy=retry_policy,
        owners=owners,
    )
    
    # Créer le schedule adaptatif et le job
    sqlmesh_adaptive_schedule, sqlmesh_job, _ = sqlmesh_adaptive_schedule_factory(
        sqlmesh_resource=sqlmesh_resource,
        name=schedule_name
    )
    
    # Retourner les Definitions complètes
    return Definitions(
        assets=[sqlmesh_assets],
        jobs=[sqlmesh_job],
        schedules=[sqlmesh_adaptive_schedule],
        resources={
            "sqlmesh": sqlmesh_resource,
        },
    ) 