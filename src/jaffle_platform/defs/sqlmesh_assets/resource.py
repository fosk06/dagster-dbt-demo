from dagster import ConfigurableResource, get_dagster_logger
from pydantic import Field
from sqlmesh.core.context import Context
import dagster as dg
from .translator import SQLMeshTranslator
from .config import SQLMeshContextConfig
from .sqlmesh_asset_utils import (
    extract_metadata,
    extract_plan_metadata,
    get_models_to_materialize,
    get_assetkey_to_snapshot,
    get_topologically_sorted_asset_keys,
    has_breaking_changes,
)
from typing import Any, Optional

class SQLMeshResource(ConfigurableResource):
    project_dir: str
    gateway: Optional[str] = None
    config_override: Optional[dict[str, Any]] = None
    target: str = Field(
        default="prod",
        description=(
            "The SQLMesh target to use for execution, prod by default"
        ),
    )
    allow_breaking_changes: bool = Field(
        default=True,
        description="Allow materialization even if breaking changes are detected (default: True). Set to False to abort materialization."
    )

    @property
    def config(self) -> SQLMeshContextConfig:
        return SQLMeshContextConfig(
            project_dir=self.project_dir,
            gateway=self.gateway,
            config_override=self.config_override,
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
        # Utilise la config pour initialiser le contexte SQLMesh, supporte gateway et config_override
        context_kwargs = {"paths": self.config.project_dir}
        if self.config.gateway:
            context_kwargs["gateway"] = self.config.gateway
        if self.config.sqlmesh_config:
            context_kwargs["config"] = self.config.sqlmesh_config
        return Context(**context_kwargs)
    
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
        If breaking changes are detected and allow_breaking_changes is False, logs details and raises an exception to abort materialization.
        Uses context.log if available for logging.
        """
        model_names = [m.name for m in models]
        if not self.allow_breaking_changes:
            plan = self.context.plan(
                environment=self.target,
                select_models=model_names,
                auto_apply=False,
            )
            has_changes = has_breaking_changes(plan, self.logger, context=context)
            if has_changes:
                raise Exception(
                    f"Breaking changes detected in plan {getattr(plan, 'plan_id', None)}. "
                    "Materialization aborted. See logs for details."
                )
            else:
                self.context.apply(plan)
        else:
            plan = self.context.plan(
                environment=self.target,
                select_models=model_names,
                auto_apply=True,
                no_prompts=True,
                no_auto_categorization=False,
            )
        return plan

    def materialize_all_assets(self, context):
        """
        Materialize all selected SQLMesh assets for Dagster in a single, centralized method.
        Handles selection, materialization, snapshot extraction, topological ordering,
        and yields AssetMaterialization and Output for each asset.
        """
        selected_asset_keys = context.selected_asset_keys
        models_to_materialize = get_models_to_materialize(
            selected_asset_keys,
            self.get_models,
            self.translator,
        )
        plan = self.materialize_assets(models_to_materialize, context=context)
        plan_metadata = extract_plan_metadata(plan)
        assetkey_to_snapshot = get_assetkey_to_snapshot(self.context, self.translator)
        ordered_asset_keys = get_topologically_sorted_asset_keys(
            self.context, self.translator, selected_asset_keys
        )

        # Log plan metadata once globally
        if context and hasattr(context, "log"):
            context.log.info(f"SQLMesh plan metadata: {plan_metadata}")
        else:
            self.logger.info(f"SQLMesh plan metadata: {plan_metadata}")

        for asset_key in ordered_asset_keys:
            snapshot = assetkey_to_snapshot.get(asset_key)
            yield dg.AssetMaterialization(
                asset_key=asset_key,
                metadata={"sqlmesh_snapshot_version": getattr(snapshot, "version", None)},
            )
            yield dg.Output(
                value=None,  # Replace with actual value if available
                output_name=asset_key.to_python_identifier(),
                data_version=dg.DataVersion(str(getattr(snapshot, "version", ""))) if snapshot else None,
                metadata={"sqlmesh_snapshot_version": getattr(snapshot, "version", None)}
            )