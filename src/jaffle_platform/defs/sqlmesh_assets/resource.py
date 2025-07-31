import threading
import anyio
import logging
import datetime
from typing import Any, Optional, List
from dagster import (
    ConfigurableResource, 
    MaterializeResult, 
    DataVersion, 
    AssetExecutionContext, 
    AssetKey, 
    AssetMaterialization, 
    AssetObservation,
)
from sqlmesh import Context
from sqlmesh.core.console import set_console, Verbosity, Console
from .translator import SQLMeshTranslator
from .sqlmesh_asset_utils import (
    get_models_to_materialize,
    extract_plan_metadata,
    get_topologically_sorted_asset_keys,
    has_breaking_changes,
    has_breaking_changes_with_message,
    format_partition_metadata,
    get_model_partitions_from_plan,
)
from .sqlmesh_event_console import SQLMeshEventCaptureConsole
from sqlmesh.core.model import Model
from sqlmesh.core.plan import Plan
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
from sqlmesh.utils.concurrency import NodeExecutionFailedError
import time

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

class BreakingChangeError(Exception):
    """Exception levée quand des changements breaking sont détectés dans SQLMesh."""
    def __init__(self, message: str = "Breaking change detected, no materialization will be done"):
        self.message = message
        super().__init__(self.message)


class SQLMeshResource(ConfigurableResource):
    """
    Resource Dagster pour interagir avec SQLMesh.
    Gère le contexte SQLMesh, le caching et orchestre la matérialisation.
    """
    
    project_dir: str
    gateway: str = "postgres"
    allow_breaking_changes: bool = False
    concurrency_limit: int = 1
    
    # Singleton pour la console SQLMesh (initialisé de manière lazy)
    
    def __init__(self, **kwargs):
        # Extraire le translator avant d'appeler super().__init__
        translator = kwargs.pop('translator', None)
        super().__init__(**kwargs)
        
        # Stocker le translator pour utilisation ultérieure
        if translator:
            self._translator_instance = translator
            
        # Initialiser l'ID unique pour cette instance
        self._instance_id = id(self)
        
        # Créer la console SQLMesh dès l'initialisation
        self._console = self._get_or_create_console()

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
            console.logger = self.logger  # Mettre à jour le logger
            
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
        Matérialise les assets SQLMesh spécifiés avec gestion d'erreurs robuste.
        """
        model_names = [model.name for model in models]
        
        # S'assurer que notre console est active pour SQLMesh
        set_console(self._console)
        self._console.clear_events()
        
        # Le logger est déjà configuré dans la console, pas besoin de le changer
        try:
            plan = self.context.plan(
                select_models=model_names,
                auto_apply=True,
                no_prompts=True
            )
            has_changes, breaking_changes_msg = has_breaking_changes_with_message(plan, self.logger, self.context)
            if has_changes:
                raise BreakingChangeError(breaking_changes_msg)
            else:
                self.context.run(
                    ignore_cron=False,
                    select_models=model_names,
                    execution_time=datetime.datetime.now()
                )
            self.logger.debug("Audit results:")
            self.logger.debug(self._console.get_audit_results())
            return plan

        except BreakingChangeError as e:
            self.logger.error(f"Changement breaking détecté : {e.message}")
            raise  # Re-raise pour que Dagster puisse gérer cette erreur spécifique
        except CircuitBreakerError:
            self.logger.error("Run interrompu : l'environnement a changé pendant l'exécution.")
        except (PlanError, ConflictingPlanError, NoChangesPlanError, UncategorizedPlanError) as e:
            self.logger.error(f"Erreur de planification : {e}")
        except (AuditError, NodeAuditsErrors) as e:
            self.logger.error(f"Erreur d'audit : {e}")
        except (PythonModelEvalError, SignalEvalError) as e:
            self.logger.error(f"Erreur d'exécution de modèle ou de signal : {e}")
        except SQLMeshError as e:
            self.logger.error(f"Erreur SQLMesh : {e}")
        except Exception as e:
            self.logger.error(f"Erreur inattendue : {e}")

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
        
        # Créer et appliquer le plan
        plan = self.materialize_assets_threaded(models_to_materialize, context=context)
        plan_metadata = extract_plan_metadata(plan)
        
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