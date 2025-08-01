import threading
import anyio
import logging
import datetime
from typing import Any
from pydantic import PrivateAttr
from dagster import (
    ConfigurableResource, 
    MaterializeResult, 
    DataVersion, 
    AssetCheckResult,
    get_dagster_logger,
    InitResourceContext
)
from sqlmesh import Context
from sqlmesh.core.console import set_console
from .translator import SQLMeshTranslator
from .sqlmesh_asset_utils import (
    get_models_to_materialize,
    get_topologically_sorted_asset_keys,
    format_partition_metadata,
    get_model_partitions_from_plan,
    analyze_sqlmesh_crons_using_api,
)
from .sqlmesh_event_console import SQLMeshEventCaptureConsole
from sqlmesh.utils.errors import (
    SQLMeshError,
    PlanError,
    ConflictingPlanError,
    NodeAuditsErrors,
    CircuitBreakerError,
    NoChangesPlanError,
    UncategorizedPlanError,
    AuditError,
    PythonModelEvalError,
    SignalEvalError,
)

def convert_unix_timestamp_to_readable(timestamp):
    """
    Convertit un timestamp Unix en date lisible.
    
    Args:
        timestamp: Timestamp Unix en millisecondes (int ou float)
        
    Returns:
        str: Date au format "YYYY-MM-DD HH:MM:SS" ou None si timestamp est None
    """
    if timestamp is None:
        return None
    
    try:
        # Convertir les millisecondes en secondes
        timestamp_seconds = timestamp / 1000
        dt = datetime.datetime.fromtimestamp(timestamp_seconds)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        # Fallback si la conversion échoue
        return str(timestamp)


# Lock global pour le singleton de la console SQLMesh
_console_lock = threading.Lock()


class SQLMeshResource(ConfigurableResource):
    """
    Resource Dagster pour interagir avec SQLMesh.
    Gère le contexte SQLMesh, le caching et orchestre la matérialisation.
    """
    
    project_dir: str
    gateway: str = "postgres"
    concurrency_limit: int = 1
    ignore_cron: bool = False
    
    # Attribut privé pour le logger Dagster (non soumis à l'immuabilité Pydantic)
    _logger: Any = PrivateAttr(default=None)
    
    # Singleton pour la console SQLMesh (initialisé de manière lazy)
    
    def __init__(self, **kwargs):
        # Extraire le translator avant d'appeler super().__init__
        translator = kwargs.pop('translator', None)
        super().__init__(**kwargs)
        
        # Stocker le translator pour utilisation ultérieure
        if translator:
            self._translator_instance = translator
            
        # Créer la console SQLMesh dès l'initialisation
        self._console = self._get_or_create_console()
        
        # Configurer le translator dans la console après sa création
        if hasattr(self, '_translator_instance') and self._translator_instance:
            self._console._translator = self._translator_instance
        

    def __del__(self):
        pass  # Cleanup simplifié

    @property
    def logger(self):
        """Retourne le logger pour cette resource."""
        return logging.getLogger(__name__)

    @classmethod
    def _get_or_create_console(cls) -> 'SQLMeshEventCaptureConsole':
        """Crée ou retourne l'instance singleton de la console SQLMesh événementielle."""
        # Initialiser les variables de classe de manière lazy
        if not hasattr(cls, '_console_instance'):
            cls._console_instance = None
        
        if cls._console_instance is None:
            with _console_lock:
                if cls._console_instance is None:  # Double-check pattern
                    cls._console_instance = SQLMeshEventCaptureConsole(
                        log_override=logging.getLogger(__name__),
                    )
                    set_console(cls._console_instance)
        return cls._console_instance

    @property
    def context(self) -> Context:
        """
        Retourne le contexte SQLMesh. Cached pour les performances.
        """
        if not hasattr(self, '_context_cache'):
            # Configurer la console custom avant de créer le contexte
            console = self._get_or_create_console()
            console._dagster_logger = self._logger  # Mettre à jour le logger
            
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

    def setup_for_execution(self, context: InitResourceContext) -> None:
        # Stocker le logger Dagster dans l'attribut privé
        self._logger = context.log
        
        # Configurer la console avec le logger Dagster
        if hasattr(self, '_console') and self._console:
            self._console._dagster_logger = self._logger

    def get_models(self):
        """
        Retourne tous les modèles SQLMesh. Cached pour les performances.
        """
        if not hasattr(self, '_models_cache'):
            self._models_cache = list(self.context.models.values())
        return self._models_cache

    def get_recommended_schedule(self):
        """
        Analyse les crons SQLMesh et retourne le schedule Dagster recommandé.
        
        Returns:
            str: Expression cron Dagster recommandée
        """
        return analyze_sqlmesh_crons_using_api(self.context)

    def _serialize_audit_args(self, audit_args):
        """
        Sérialise les arguments d'audit en format JSON-compatible.
        """
        if not audit_args:
            return {}
        
        serialized = {}
        for key, value in audit_args.items():
            try:
                # Essayer de convertir en string si c'est un objet complexe
                if hasattr(value, '__str__'):
                    serialized[key] = str(value)
                elif hasattr(value, '__dict__'):
                    # Pour les objets avec __dict__, extraire les attributs principaux
                    serialized[key] = {k: str(v) for k, v in value.__dict__.items() if not k.startswith('_')}
                else:
                    # Fallback: conversion directe
                    serialized[key] = str(value)
            except Exception:
                # En cas d'erreur, utiliser une représentation simple
                serialized[key] = f"<non-serializable: {type(value).__name__}>"
        
        return serialized

    def materialize_assets(self, models, context=None):
        """
        Matérialise les assets SQLMesh spécifiés avec gestion d'erreurs robuste.
        """
        model_names = [model.name for model in models]
        # S'assurer que notre console est active pour SQLMesh
        set_console(self._console)
        self._console.clear_events()
        
        try:
            plan = self.context.plan(
                select_models=model_names,
                auto_apply=False, # never apply the plan, we will juste need it for metadata collection
                no_prompts=True
            )
            self.context.run(
                ignore_cron=self.ignore_cron,
                select_models=model_names,
                execution_time=datetime.datetime.now(),
            )
            return plan

        except CircuitBreakerError:
            self._logger.error("Run interrompu : l'environnement a changé pendant l'exécution.")
            raise
        except (PlanError, ConflictingPlanError, NoChangesPlanError, UncategorizedPlanError) as e:
            self._logger.error(f"Erreur de planification : {e}")
            raise
        except (AuditError, NodeAuditsErrors) as e:
            self._logger.error(f"Erreur d'audit : {e}")
            raise
        except (PythonModelEvalError, SignalEvalError) as e:
            self._logger.error(f"Erreur d'exécution de modèle ou de signal : {e}")
            raise
        except SQLMeshError as e:
            self._logger.error(f"Erreur SQLMesh : {e}")
            raise
        except Exception as e:
            self._logger.error(f"Erreur inattendue : {e}")
            raise

    def materialize_assets_threaded(self, models, context=None):
        """
        Wrapper synchrone pour Dagster qui utilise anyio.
        """

        def run_materialization():
            try:
                return self.materialize_assets(models, context)
            except Exception as e:
                self._logger.error(f"Materialization failed: {e}")
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
        
        # Créer et appliquer le plan
        plan = self.materialize_assets_threaded(models_to_materialize, context=context)
        
        # Extraire les snapshots catégorisés directement depuis le plan
        assetkey_to_snapshot = {}
        for snapshot in plan.snapshots.values():
            model = snapshot.model
            asset_key = self.translator.get_asset_key(model)
            assetkey_to_snapshot[asset_key] = snapshot
        
        # Trier les asset keys dans l'ordre topologique
        ordered_asset_keys = get_topologically_sorted_asset_keys(
            self.context, self.translator, selected_asset_keys
        )

        # Créer les MaterializeResult avec les infos du plan
        for asset_key in ordered_asset_keys:
            snapshot = assetkey_to_snapshot.get(asset_key)
            if snapshot:
                snapshot_version = getattr(snapshot, "version", None)
                model_partitions = get_model_partitions_from_plan(plan, self.translator, asset_key, snapshot)
                # Préparer les métadonnées de base
                metadata = {
                    "dagster-sqlmesh/snapshot_version": snapshot_version,
                    "dagster-sqlmesh/snapshot_timestamp": convert_unix_timestamp_to_readable(getattr(snapshot, "created_ts", None)) if snapshot else None,
                    "dagster-sqlmesh/model_name": asset_key.path[-1] if asset_key.path else None,
                }
                
                # Ajouter les métadonnées de partition si le modèle est partitionné
                if model_partitions and model_partitions.get("is_partitioned", False):
                    metadata["dagster-sqlmesh/partitions"] = format_partition_metadata(model_partitions)
                
                yield MaterializeResult(
                    asset_key=asset_key,
                    metadata=metadata,
                    data_version=DataVersion(str(snapshot_version)) if snapshot_version else None
                )
        
        # Émettre les AssetCheckResult après tous les MaterializeResult
        audit_results = self._console.get_audit_results()
        for audit_result in audit_results:
            audit_details = audit_result['audit_details']
            asset_key = audit_result['asset_key']
            
            # Déterminer si l'audit a passé (pour l'instant on assume True, on affinera plus tard)
            passed = True  # TODO: déterminer le vrai statut basé sur les événements
            
            # Sérialiser les arguments d'audit en format JSON-compatible
            serialized_args = self._serialize_audit_args(audit_details['arguments'])
            
            yield AssetCheckResult(
                passed=passed,
                asset_key=asset_key,
                check_name=audit_details['name'],
                metadata={
                    "sqlmesh_model_name": audit_result['model_name'],  # ← Nom du modèle SQLMesh
                    "audit_query": audit_details['sql'],
                    "audit_blocking": audit_details['blocking'],
                    "audit_dialect": getattr(audit_details, 'dialect', 'unknown'),
                    "audit_args": serialized_args
                }
            )
        
        # Nettoyer les événements de la console après avoir émis tous les AssetCheckResult
        self._console.clear_events()