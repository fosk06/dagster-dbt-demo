import dagster as dg
from .translator import SQLMeshTranslator
from .resource import SQLMeshResource
from .sqlmesh_asset_utils import (
    get_assetkey_to_snapshot,
    get_asset_kinds,
    get_asset_group_name,
    get_asset_tags,
    get_asset_metadata,
    validate_external_dependencies,
    get_all_external_asset_keys,
)
from typing import Any, Optional

def sqlmesh_assets_factory(
    *,
    sqlmesh_resource: SQLMeshResource,
    name: str = "sqlmesh_assets",
    group_name: str = "sqlmesh",
    translator: SQLMeshTranslator = None,
    op_tags: dict = None,
    required_resource_keys: set = None,
    retry_policy: dg.RetryPolicy = None,
    owners: list = None,
):
    """
    Factory pour créer des assets SQLMesh Dagster.
    
    Args:
        sqlmesh_resource: La resource SQLMesh configurée
        name: Nom du multi_asset
        group_name: Groupe par défaut pour les assets
        translator: Translator custom (optionnel)
        op_tags: Tags pour l'opération
        required_resource_keys: Clés de resources requises
        retry_policy: Politique de retry
        owners: Propriétaires des assets
    """
    # Utilise le translator fourni ou celui de la resource
    translator = translator or sqlmesh_resource.translator

    # Pre-calculate expensive operations once
    models = list(sqlmesh_resource.get_models())
    context = sqlmesh_resource.context
    assetkey_to_snapshot = get_assetkey_to_snapshot(context, translator)
    extra_keys = ["cron", "tags", "kind", "dialect", "query", "partitioned_by", "clustered_by"]

    # Pre-calculate common values
    kinds = get_asset_kinds(translator, context)

    # Validate external dependencies before creating specs
    validation_errors = validate_external_dependencies(context, translator, models)
    if validation_errors:
        raise ValueError(f"External dependency validation failed:\n" + "\n".join(validation_errors))

    # Log external asset keys for debugging
    external_keys = get_all_external_asset_keys(context, translator, models)
    if external_keys:
        print(f"Found external asset keys: {external_keys}")

    specs = []
    for model in models:
        asset_key = translator.get_asset_key(model)
        snapshot = assetkey_to_snapshot.get(asset_key)
        code_version = str(getattr(snapshot, "version", None)) if snapshot else None
        metadata = get_asset_metadata(translator, model, code_version, extra_keys, owners)
        tags = get_asset_tags(translator, context, model)

        # Use the new method that handles external assets
        deps = translator.get_model_deps_with_external(context, model)

        specs.append(
            dg.AssetSpec(
                key=asset_key,
                deps=deps,  # Now includes both internal and external dependencies
                code_version=code_version,
                metadata=metadata,
                kinds=kinds,
                tags=tags,
                group_name=group_name,  # Use the factory parameter
            )
        )

    @dg.multi_asset(
        name=name,
        specs=specs,
        op_tags=op_tags,
        required_resource_keys=required_resource_keys,
        retry_policy=retry_policy,
    )
    def _sqlmesh_assets(context: dg.AssetExecutionContext, sqlmesh: SQLMeshResource):
        yield from sqlmesh.materialize_all_assets(context)

    return _sqlmesh_assets 