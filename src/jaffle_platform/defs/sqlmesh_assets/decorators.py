from dagster import (
    AssetSpec,
    AssetCheckSpec,
    multi_asset,
    AssetExecutionContext,
    RetryPolicy,
    schedule,
)
from .resource import SQLMeshResource
from .sqlmesh_asset_utils import (
    get_assetkey_to_snapshot,
    get_asset_kinds,
    get_asset_tags,
    get_asset_metadata,
    validate_external_dependencies,
    get_all_external_asset_keys,
    get_model_partitions,
    get_extra_keys,
    create_asset_specs,
    create_asset_checks,
)
from typing import Any, Optional
from sqlmesh.core.model.definition import ExternalModel
import datetime

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
    schedule = sqlmesh_resource.get_recommended_schedule()

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
    
    @schedule(
        job=sqlmesh_assets_factory(sqlmesh_resource=sqlmesh_resource),
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
    
    return _sqlmesh_adaptive_schedule 