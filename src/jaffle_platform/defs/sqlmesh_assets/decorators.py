from dagster import (
    multi_asset,
    AssetExecutionContext,
    RetryPolicy,
    schedule,
    define_asset_job,
    RunRequest,
    Definitions,
)
from .resource import SQLMeshResource
from .sqlmesh_asset_utils import (
    get_asset_kinds,
    get_extra_keys,
    create_asset_specs,
    create_asset_checks,
)
import datetime
from .translator import SQLMeshTranslator

def sqlmesh_assets_factory(
    *,
    sqlmesh_resource: SQLMeshResource,
    name: str = "sqlmesh_assets",
    group_name: str = "sqlmesh",
    op_tags: dict = None,
    required_resource_keys: set = None,
    retry_policy: RetryPolicy = None,
    owners: list = None,
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
    extra_keys = get_extra_keys()
    kinds = get_asset_kinds(sqlmesh_resource)

    # Créer les AssetSpec et AssetCheckSpec
    specs = create_asset_specs(sqlmesh_resource, extra_keys, kinds, owners, group_name)
    asset_checks = create_asset_checks(sqlmesh_resource)

    @multi_asset(
        name=name,
        specs=specs,
        check_specs=asset_checks,
        op_tags=op_tags,
        required_resource_keys=required_resource_keys,
        retry_policy=retry_policy,
        can_subset=True
    )
    def _sqlmesh_assets(context: AssetExecutionContext, sqlmesh: SQLMeshResource):

        yield from sqlmesh.materialize_all_assets(context)

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
    
    # Créer automatiquement le job SQLMesh
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
        """
        Schedule adaptatif qui s'exécute selon la granularité la plus fine des modèles SQLMesh.
        SQLMesh gère automatiquement quels modèles doivent être exécutés.
        """
        
        # SQLMesh gère tout automatiquement !
        # On lance juste un "sqlmesh run" sur tous les modèles
        sqlmesh_resource.context.run(
            ignore_cron=False,  # SQLMesh respecte les crons
            execution_time=datetime.datetime.now(),
        )
        
        context.log.info(f"✅ Schedule adaptatif exécuté avec granularité: {recommended_schedule}")
        context.log.debug(f"📊 Modèles analysés: {len(sqlmesh_resource.get_models())} modèles")
        
        # Retourner un RunRequest pour déclencher le job Dagster
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
    translator: SQLMeshTranslator = None,
    name: str = "sqlmesh_assets",
    group_name: str = "sqlmesh",
    op_tags: dict = None,
    required_resource_keys: set = None,
    retry_policy: RetryPolicy = None,
    owners: list = None,
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
    
    # Créer la resource SQLMesh (breaking changes jamais autorisés)
    sqlmesh_resource = SQLMeshResource(
        project_dir=project_dir,
        gateway=gateway,
        translator=translator,
        concurrency_limit=concurrency_limit,
        ignore_cron=ignore_cron
    )
    
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