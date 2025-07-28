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
from typing import Any, Optional, Callable
import threading
import anyio

class SQLMeshResource(ConfigurableResource):
    """
    Resource Dagster pour interagir avec SQLMesh.
    Gère le contexte SQLMesh, le caching et orchestre la matérialisation.
    """
    
    project_dir: str
    gateway: str = "postgres"
    config_override: Optional[dict] = None
    allow_breaking_changes: bool = False
    translator: Optional[SQLMeshTranslator] = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._context_cache = None
        self._models_cache = None
        self._translator_cache = None
        self._instance_id = id(self)

        # Singleton strict control - using class variables outside of Pydantic fields
        if not hasattr(SQLMeshResource, '_instance_lock'):
            SQLMeshResource._instance_lock = threading.Lock()
            SQLMeshResource._active_instances = set()

        with self._instance_lock:
            if self._instance_id in self._active_instances:
                raise Exception("Only one SQLMesh instance allowed at a time")
            self._active_instances.add(self._instance_id)

    def __del__(self):
        if hasattr(self, '_instance_id'):
            with self._instance_lock:
                self._active_instances.discard(self._instance_id)

    @property
    def context(self) -> Context:
        """
        Retourne le contexte SQLMesh. Cached pour les performances.
        """
        if self._context_cache is None:
            self._context_cache = Context(
                paths=self.project_dir,
                gateway=self.gateway,
                config_override=self.config_override,
            )
        return self._context_cache

    @property
    def translator(self) -> SQLMeshTranslator:
        """
        Retourne une instance SQLMeshTranslator pour mapper AssetKeys et modèles.
        Cached pour les performances.
        """
        if self._translator_cache is None:
            self._translator_cache = self.translator or SQLMeshTranslator()
        return self._translator_cache

    def get_models(self):
        """
        Retourne tous les modèles SQLMesh. Cached pour les performances.
        """
        if self._models_cache is None:
            self._models_cache = list(self.context.models.values())
        return self._models_cache

    def materialize_assets(self, models, context=None):
        """
        Matérialise les assets SQLMesh spécifiés.
        """
        plan = self.context.plan(
            models=models,
            allow_breaking_changes=self.allow_breaking_changes,
        )
        
        if plan.requires_backfill:
            self.logger.info("Backfill required, applying plan...")
            self.context.apply(plan)
        else:
            self.logger.info("No backfill required, applying plan...")
            self.context.apply(plan)
        
        return plan

    async def materialize_assets_async(self, models, context=None):
        """
        Version asynchrone de materialize_assets utilisant anyio.
        """
        def run_materialization():
            try:
                return self.materialize_assets(models, context)
            except Exception as e:
                self.logger.error(f"Materialization failed: {e}")
                raise
        return await anyio.to_thread.run_sync(run_materialization)

    def materialize_assets_threaded(self, models, context=None):
        """
        Wrapper synchrone pour Dagster qui utilise anyio.
        """
        def run_materialization():
            try:
                return self.materialize_assets(models, context)
            except Exception as e:
                self.logger.error(f"Materialization failed: {e}")
                raise
        return anyio.run(anyio.to_thread.run_sync, run_materialization)

    def materialize_all_assets(self, context):
        """
        Matérialise tous les assets sélectionnés et yield les résultats.
        """
        selected_asset_keys = context.selected_asset_keys
        models_to_materialize = get_models_to_materialize(
            selected_asset_keys,
            self.get_models,
            self.translator,
        )
        plan = self.materialize_assets_threaded(models_to_materialize, context=context)
        plan_metadata = extract_plan_metadata(plan)
        assetkey_to_snapshot = get_assetkey_to_snapshot(self.context, self.translator)
        ordered_asset_keys = get_topologically_sorted_asset_keys(
            self.context, self.translator, selected_asset_keys
        )

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
                value=None,
                output_name=asset_key.to_python_identifier(),
                data_version=dg.DataVersion(str(getattr(snapshot, "version", ""))) if snapshot else None,
                metadata={"sqlmesh_snapshot_version": getattr(snapshot, "version", None)}
            )