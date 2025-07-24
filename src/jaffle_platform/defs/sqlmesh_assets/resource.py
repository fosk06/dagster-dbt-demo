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
        return self.context.run(**kwargs)

    def plan(self, **kwargs):
        return self.context.plan(**kwargs)

    def apply(self, plan, **kwargs):
        return self.context.apply(plan, **kwargs)

    def audit(self, **kwargs):
        return self.context.audit(**kwargs)

    def test(self, **kwargs):
        return self.context.test(**kwargs)

    def lint_models(self, **kwargs):
        return self.context.lint_models(**kwargs)

    def diff(self, **kwargs):
        return self.context.diff(**kwargs)

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

    def materialize_assets(self, models):
        """
        Materialize the given list of SQLMesh models using run (plan + apply).
        """
        model_names = [m.name for m in models]
        plan = self.context.plan(select_models=model_names, auto_apply=True)
        self.context.apply(plan)
        return plan

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