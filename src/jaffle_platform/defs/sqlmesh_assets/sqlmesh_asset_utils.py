# Utility functions for SQLMeshResource and Dagster integration

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


def get_models_to_materialize(selected_asset_keys, get_models, translator) -> list:
    """
    Returns the list of SQLMesh models corresponding to the selected AssetKeys.
    get_models: callable returning all models
    translator: SQLMeshTranslator instance
    """
    models = list(get_models())
    assetkey_to_model = translator.get_assetkey_to_model(models)
    return [
        assetkey_to_model[asset_key]
        for asset_key in selected_asset_keys
        if asset_key in assetkey_to_model
    ]


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