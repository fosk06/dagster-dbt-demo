import threading
import anyio
import logging
from typing import Any, Optional
import dagster as dg
from dagster import ConfigurableResource, Field, RetryPolicy, AssetKey, MaterializeResult, DataVersion, PartitionKeyRange
from datetime import datetime
from sqlmesh import Context
from .translator import SQLMeshTranslator
from .sqlmesh_asset_utils import (
    get_models_to_materialize,
    extract_plan_metadata,
    get_assetkey_to_snapshot,
    get_topologically_sorted_asset_keys,
    get_model_partitions,
)


def convert_intervals_to_partition_range(intervals) -> PartitionKeyRange:
    """
    Convertit les intervalles SQLMesh (timestamps Unix) en PartitionKeyRange Dagster.
    
    Args:
        intervals: Liste de tuples (start_timestamp, end_timestamp)
    
    Returns:
        PartitionKeyRange ou None si conversion impossible
    """
    if not intervals or len(intervals) == 0:
        return None
    
    try:
        # Prendre le premier intervalle (on pourrait étendre pour gérer plusieurs)
        start_timestamp, end_timestamp = intervals[0]
        
        # Convertir les timestamps Unix (millisecondes) en datetime
        # Les timestamps semblent être en millisecondes (13 chiffres)
        start_dt = datetime.fromtimestamp(start_timestamp / 1000)
        end_dt = datetime.fromtimestamp(end_timestamp / 1000)
        
        # Formater en string pour PartitionKeyRange
        start_str = start_dt.strftime("%Y-%m-%d")
        end_str = end_dt.strftime("%Y-%m-%d")
        
        return PartitionKeyRange(start_str, end_str)
        
    except (ValueError, TypeError, IndexError) as e:
        # Log l'erreur mais ne pas faire planter
        print(f"Erreur conversion intervals: {e}")
        return None

class SQLMeshResource(ConfigurableResource):
    """
    Resource Dagster pour interagir avec SQLMesh.
    Gère le contexte SQLMesh, le caching et orchestre la matérialisation.
    """
    
    project_dir: str
    gateway: str = "postgres"
    config_override: Optional[dict] = None
    allow_breaking_changes: bool = False

    def __init__(self, **kwargs):
        # Extraire le translator avant d'appeler super().__init__
        translator = kwargs.pop('translator', None)
        
        super().__init__(**kwargs)
        self._translator_instance = translator  # Stocke le translator fourni
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
    def logger(self):
        """Retourne le logger pour cette resource."""
        return logging.getLogger(__name__)

    @property
    def context(self) -> Context:
        """
        Retourne le contexte SQLMesh. Cached pour les performances.
        """
        if not hasattr(self, '_context_cache'):
            self._context_cache = Context(
                paths=self.project_dir,
                gateway=self.gateway,
            )
        return self._context_cache

    @property
    def translator(self) -> SQLMeshTranslator:
        """
        Retourne une instance SQLMeshTranslator pour mapper AssetKeys et modèles.
        Cached pour les performances.
        """
        if not hasattr(self, '_translator_cache'):
            # Utilise le translator fourni en paramètre ou crée un nouveau
            self._translator_cache = getattr(self, '_translator_instance', None) or SQLMeshTranslator()
        return self._translator_cache

    def get_models(self):
        """
        Retourne tous les modèles SQLMesh. Cached pour les performances.
        """
        if not hasattr(self, '_models_cache'):
            self._models_cache = list(self.context.models.values())
        return self._models_cache

    def materialize_assets(self, models, context=None):
        """
        Matérialise les assets SQLMesh spécifiés.
        """
        # Extraire les noms des modèles
        model_names = [model.name for model in models]
        
        plan = self.context.plan(
            select_models=model_names,
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
            snapshot_version = getattr(snapshot, "version", None)
            model_partitions = get_model_partitions(self.context, self.translator, asset_key, snapshot)
            
            # Préparer les métadonnées de base
            metadata = {
                "sqlmesh_snapshot_version": snapshot_version,
                "materialization_timestamp": str(getattr(snapshot, "created_ts", None)) if snapshot else None,
                "sqlmesh_model_name": asset_key.path[-1] if asset_key.path else None,
            }
            
            # Gérer les partitions SQLMesh
            yield from self._materialize_with_partitions(
                asset_key=asset_key,
                snapshot=snapshot,
                snapshot_version=snapshot_version,
                metadata=metadata
            )

    def _materialize_with_partitions(self, asset_key, snapshot, snapshot_version, metadata):
        """
        Gère la matérialisation avec ou sans partitions.
        Temporairement désactivé pour éviter les conflits de partition.
        """
        # TODO: Réactiver une fois les partitions unifiées
        # partition_info = self.translator.get_sqlmesh_partition_info(snapshot) if snapshot else {}
        
        # if partition_info.get("partitioned_by") and partition_info.get("cron"):
        #     # Asset partitionné temporellement
        #     for interval in partition_info.get("intervals", []):
        #         yield MaterializeResult(
        #             asset_key=asset_key,
        #             partition=interval,  # ex: "2024-01-15" pour @daily
        #             data_version=DataVersion(str(snapshot_version)) if snapshot_version else None,
        #             metadata={
        #                 **metadata,
        #                 "sqlmesh_snapshot_version": snapshot_version,
        #                 "partition_interval": interval,
        #                 "partition_cron": partition_info.get("cron"),
        #             }
        #         )
        # elif partition_info.get("partitioned_by"):
        #     # Asset partitionné statiquement
        #     for partition in partition_info.get("partitioned_by", []):
        #         yield MaterializeResult(
        #             asset_key=asset_key,
        #             partition=partition,
        #             data_version=DataVersion(str(snapshot_version)) if snapshot_version else None,
        #             metadata={
        #                 **metadata,
        #                 "sqlmesh_snapshot_version": snapshot_version,
        #                 "partition_column": partition,
        #             }
        #         )
        # else:
        # Asset non partitionné (temporairement tous les assets)
        yield MaterializeResult(
            asset_key=asset_key,
            metadata=metadata,
            data_version=DataVersion(str(snapshot_version)) if snapshot_version else None,
        )