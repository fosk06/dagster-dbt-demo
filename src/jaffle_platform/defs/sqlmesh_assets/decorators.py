from dagster import (
    AssetSpec,
    AssetCheckSpec,
    multi_asset,
    AssetExecutionContext,
    RetryPolicy,
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