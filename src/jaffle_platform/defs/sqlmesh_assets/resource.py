from dagster import ConfigurableResource, get_dagster_logger
from pydantic import Field
from sqlmesh.core.context import Context
from .translator import SQLMeshTranslator
import dagster as dg

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