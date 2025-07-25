from dagster import ConfigurableResource, get_dagster_logger
from pydantic import Field
from sqlmesh.core.context import Context
import dagster as dg
from .translator import SQLMeshTranslator

class SQLMeshResource(ConfigurableResource):
    project_dir: str = Field(
        description=(
            "The path to your sqlmesh project directory. This directory should contain a"
            " `config.yml` file at the root of the project."
        ),
    )
    target: str = Field(
        default="prod",
        description=(
            "The SQLMesh target to use for execution, prod by default"
        ),
    )

    @property
    def translator(self):
        """
        Returns a SQLMeshTranslator instance for mapping AssetKeys and models.
        Stateless, so always returns a new instance.
        """
        return SQLMeshTranslator()

    @property
    def context(self) -> Context:
        return Context(paths=self.project_dir)
    
    @property
    def logger(self):
        return get_dagster_logger()

    def get_models(self):
        return self.context.models.values()

    def get_model(self, name, **kwargs):
        return self.context.get_model(name, **kwargs)

    def render(self, model_or_snapshot, **kwargs):
        return self.context.render(model_or_snapshot, **kwargs)

    def evaluate(self, model_or_snapshot, **kwargs):
        return self.context.evaluate(model_or_snapshot, **kwargs)

    def run(self, **kwargs):
        """
        Run the entire dag through the scheduler for the configured target environment.
        """
        return self.context.run(environment=self.target, **kwargs)

    def plan(self, **kwargs):
        return self.context.plan(**kwargs)

    def apply(self, plan, **kwargs):
        return self.context.apply(plan, **kwargs)

    def audit(self, **kwargs):
        """
        Audit models in the configured target environment.
        """
        return self.context.audit(environment=self.target, **kwargs)

    def test(self, **kwargs):
        return self.context.test(**kwargs)

    def lint_models(self, **kwargs):
        return self.context.lint_models(**kwargs)

    def diff(self, **kwargs):
        """
        Show a diff of the current context with the configured target environment.
        """
        return self.context.diff(environment=self.target, **kwargs)

    def get_dag(self, **kwargs):
        return self.context.get_dag(**kwargs)

    def invalidate_environment(self, name, **kwargs):
        return self.context.invalidate_environment(name, **kwargs)

    def table_name(self, model_name, **kwargs):
        return self.context.table_name(model_name, **kwargs)

    def fetchdf(self, query, **kwargs):
        return self.context.fetchdf(query, **kwargs)

    def clear_caches(self):
        return self.context.clear_caches()

    def materialize_assets(self, models, context=None):
        """
        Materialize the given list of SQLMesh models using plan + apply.
        If breaking changes are detected, logs details and raises an exception to abort materialization.
        Uses context.log if available for logging.
        """
        model_names = [m.name for m in models]
        plan = self.context.plan(
            environment=self.target,
            select_models=model_names,
            auto_apply=False,
        )
        has_breaking_changes = self.has_breaking_changes(plan, context=context)
        if has_breaking_changes:
            raise Exception(
                f"Breaking changes detected in plan {getattr(plan, 'plan_id', None)}. "
                "Materialization aborted. See logs for details."
            )
        else:
            self.context.apply(plan)
        return plan

    def materialize_all_assets(self, context):
        """
        Materialize all selected SQLMesh assets for Dagster in a single, centralized method.
        Handles selection, materialization, snapshot extraction, topological ordering,
        and yields AssetMaterialization and Output for each asset.
        """
        selected_asset_keys = context.selected_asset_keys
        models_to_materialize = self.get_models_to_materialize(selected_asset_keys)
        plan = self.materialize_assets(models_to_materialize, context=context)
        plan_metadata = self.extract_plan_metadata(plan)
        assetkey_to_snapshot = self.get_assetkey_to_snapshot()
        ordered_asset_keys = self.get_topologically_sorted_asset_keys(plan, selected_asset_keys)
        for asset_key in ordered_asset_keys:
            snapshot = assetkey_to_snapshot.get(asset_key)
            yield dg.AssetMaterialization(
                asset_key=asset_key,
                metadata={**plan_metadata, "sqlmesh_snapshot_version": getattr(snapshot, "version", None)},
            )
            yield dg.Output(
                value=None,
                output_name=asset_key.to_python_identifier(),
                data_version=dg.DataVersion(str(getattr(snapshot, "version", ""))) if snapshot else None,
                metadata={"sqlmesh_snapshot_version": getattr(snapshot, "version", None)}
            )

    def extract_metadata(self, obj, fields: list[str], prefix: str = "sqlmesh_") -> dict:
        """
        Extract and format the specified fields from a SQLMesh object (plan, model, etc.)
        for use as Dagster asset metadata.

        Args:
            obj: The SQLMesh object (plan, model, etc.) to extract metadata from.
            fields: List of attribute names to extract from the object.
            prefix: String prefix to add to each metadata key (default: 'sqlmesh_').

        Returns:
            dict: {prefix+field: str(value)} for each field found on the object.
        """
        return {f"{prefix}{field}": str(getattr(obj, field, None)) for field in fields}

    def extract_plan_metadata(self, plan) -> dict:
        """
        Extracts and formats a standard set of metadata fields from a SQLMesh plan object
        for use as Dagster AssetMaterialization metadata. This includes plan_id, environment,
        start/end times, backfill info, and other plan diagnostics.

        Args:
            plan: The SQLMesh plan object to extract metadata from.

        Returns:
            dict: Metadata fields with 'sqlmesh_plan_' prefix, ready to be passed to Dagster.
        """
        fields = [
            "plan_id", "environment", "start", "end", "has_changes",
            "models_to_backfill", "requires_backfill", "modified_snapshots", "user_provided_flags"
        ]
        return self.extract_metadata(plan, fields, prefix="sqlmesh_plan_")

    def get_models_to_materialize(self, selected_asset_keys) -> list:
        """
        Returns the list of SQLMesh models corresponding to the selected AssetKeys.
        """
        models = list(self.get_models())
        assetkey_to_model = self.translator.get_assetkey_to_model(models)
        return [
            assetkey_to_model[asset_key]
            for asset_key in selected_asset_keys
            if asset_key in assetkey_to_model
        ]

    def get_assetkey_to_snapshot(self) -> dict:
        """
        Returns a mapping {AssetKey: snapshot} for all models in the current context.
        """
        assetkey_to_snapshot = {}
        for snapshot in self.context.snapshots.values():
            model = snapshot.model
            asset_key = self.translator.get_asset_key(model)
            assetkey_to_snapshot[asset_key] = snapshot
        return assetkey_to_snapshot

    def get_topologically_sorted_asset_keys(self, plan, selected_asset_keys) -> list:
        """
        Returns the selected_asset_keys sorted in topological order according to the SQLMesh DAG.
        """
        models = list(self.get_models())
        assetkey_to_model = self.translator.get_assetkey_to_model(models)
        # Utilise FQN comme clé
        fqn_to_model = {model.fqn: model for model in models}
        fqn_to_assetkey = {model.fqn: self.translator.get_asset_key(model) for model in models}
        # FQN sélectionnés
        selected_fqns = set(model.fqn for key, model in assetkey_to_model.items() if key in selected_asset_keys)
        topo_fqns = self.context.dag.sorted
        ordered_asset_keys = [
            fqn_to_assetkey[fqn]
            for fqn in topo_fqns
            if fqn in selected_fqns and fqn in fqn_to_assetkey
        ]
        return ordered_asset_keys
    
    def has_breaking_changes(self, plan, context=None) -> bool:
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
                self.logger.error(msg)
        else:
            info_msg = f"No breaking changes detected in plan {getattr(plan, 'plan_id', None)}."
            if context and hasattr(context, "log"):
                context.log.info(info_msg)
            else:
                self.logger.info(info_msg)

        return has_changes