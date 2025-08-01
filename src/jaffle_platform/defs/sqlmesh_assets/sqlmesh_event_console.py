import inspect
import logging
import textwrap
import typing as t
import uuid
from dataclasses import dataclass, field

from sqlmesh.core.console import Console
from sqlmesh.core.plan import EvaluatablePlan
from sqlmesh.core.snapshot import Snapshot
from .sqlmesh_asset_utils import safe_extract_audit_query

logger = logging.getLogger(__name__)

# =============================================================================
# ÉVÉNEMENTS SQLMESH (basé sur dagster-sqlmesh)
# =============================================================================

@dataclass(kw_only=True)
class BaseConsoleEvent:
    unknown_args: dict[str, t.Any] = field(default_factory=dict)

@dataclass(kw_only=True)
class StartPlanEvaluation(BaseConsoleEvent):
    plan: EvaluatablePlan

@dataclass(kw_only=True)
class StopPlanEvaluation(BaseConsoleEvent):
    pass

@dataclass(kw_only=True)
class StartSnapshotEvaluationProgress(BaseConsoleEvent):
    snapshot: Snapshot

@dataclass(kw_only=True)
class UpdateSnapshotEvaluationProgress(BaseConsoleEvent):
    snapshot: Snapshot
    batch_idx: int
    duration_ms: int | None
    num_audits_passed: int | None = None
    num_audits_failed: int | None = None

@dataclass(kw_only=True)
class StopEvaluationProgress(BaseConsoleEvent):
    success: bool = True

@dataclass(kw_only=True)
class LogStatusUpdate(BaseConsoleEvent):
    message: str

@dataclass(kw_only=True)
class LogError(BaseConsoleEvent):
    message: str

@dataclass(kw_only=True)
class LogWarning(BaseConsoleEvent):
    short_message: str
    long_message: str | None = None

@dataclass(kw_only=True)
class LogSuccess(BaseConsoleEvent):
    message: str

@dataclass(kw_only=True)
class LogFailedModels(BaseConsoleEvent):
    errors: list[t.Any]  # NodeExecutionFailedError

@dataclass(kw_only=True)
class LogSkippedModels(BaseConsoleEvent):
    snapshot_names: set[str]

@dataclass(kw_only=True)
class ConsoleException(BaseConsoleEvent):
    exception: Exception

# Union de tous les événements possibles
ConsoleEvent = (
    StartPlanEvaluation
    | StopPlanEvaluation
    | StartSnapshotEvaluationProgress
    | UpdateSnapshotEvaluationProgress
    | StopEvaluationProgress
    | LogStatusUpdate
    | LogError
    | LogWarning
    | LogSuccess
    | LogFailedModels
    | LogSkippedModels
    | ConsoleException
)

ConsoleEventHandler = t.Callable[[ConsoleEvent], None]

# =============================================================================
# CONSOLE ÉVÉNEMENTIELLE (basé sur dagster-sqlmesh)
# =============================================================================

def get_console_event_by_name(event_name: str) -> type[ConsoleEvent] | None:
    """Get the console event class by name."""
    known_events_classes = t.get_args(ConsoleEvent)
    console_event_map: dict[str, type[ConsoleEvent]] = {
        event.__name__: event for event in known_events_classes
    }
    return console_event_map.get(event_name)

class IntrospectingConsole(Console):
    """Une console qui implémente dynamiquement les méthodes basées sur les événements SQLMesh"""

    events: t.ClassVar[list[type[ConsoleEvent]]] = [
        StartPlanEvaluation,
        StopPlanEvaluation,
        StartSnapshotEvaluationProgress,
        UpdateSnapshotEvaluationProgress,
        StopEvaluationProgress,
        LogStatusUpdate,
        LogError,
        LogWarning,
        LogSuccess,
        LogFailedModels,
        LogSkippedModels,
        ConsoleException,
    ]

    def __init_subclass__(cls):
        super().__init_subclass__()

        known_events_classes = cls.events
        known_events: list[str] = []
        for known_event in known_events_classes:
            assert inspect.isclass(known_event), "event must be a class"
            known_events.append(known_event.__name__)

        # Créer dynamiquement les méthodes pour chaque événement
        for method_name in Console.__abstractmethods__:
            if hasattr(cls, method_name):
                if not getattr(getattr(cls, method_name), '__isabstractmethod__', False):
                    logger.debug(f"Skipping {method_name} as it is abstract")
                    continue
            
            logger.debug(f"Checking {method_name}")

            # Convertir snake_case en camelCase
            camel_case_method_name = "".join(
                word.capitalize()
                for _, word in enumerate(method_name.split("_"))
            )

            if camel_case_method_name in known_events:
                logger.debug(f"Creating {method_name} for {camel_case_method_name}")
                signature = inspect.signature(getattr(Console, method_name))
                handler = cls.create_event_handler(method_name, camel_case_method_name, signature)
                setattr(cls, method_name, handler)
            else:
                logger.debug(f"Creating {method_name} for unknown event")
                signature = inspect.signature(getattr(Console, method_name))
                handler = cls.create_unknown_event_handler(method_name, signature)
                setattr(cls, method_name, handler)

    @classmethod
    def create_event_handler(cls, method_name: str, event_name: str, signature: inspect.Signature):
        func_signature, call_params = cls.create_signatures_and_params(signature)

        event_handler_str = textwrap.dedent(f"""
        def {method_name}({", ".join(func_signature)}):
            self.publish_known_event('{event_name}', {", ".join(call_params)})
        """)
        exec(event_handler_str)
        return t.cast(t.Callable[[t.Any], t.Any], locals()[method_name])

    @classmethod
    def create_signatures_and_params(cls, signature: inspect.Signature):
        func_signature: list[str] = []
        call_params: list[str] = []
        
        # Séparer les paramètres avec et sans valeurs par défaut
        required_params = []
        optional_params = []
        
        for param_name, param in signature.parameters.items():
            if param_name == "self":
                func_signature.append("self")
                continue

            param_type_name = param.annotation
            if not isinstance(param_type_name, str):
                param_type_name = param_type_name.__name__
            
            if param.default is inspect._empty:
                # Paramètre requis (sans valeur par défaut)
                required_params.append((param_name, f"{param_name}: '{param_type_name}'"))
            else:
                # Paramètre optionnel (avec valeur par défaut)
                default_value = param.default
                if isinstance(param.default, str):
                    default_value = f"'{param.default}'"
                optional_params.append((param_name, f"{param_name}: '{param_type_name}' = {default_value}"))
            
            call_params.append(f"{param_name}={param_name}")
        
        # Ajouter d'abord les paramètres requis, puis les optionnels
        for _, sig in required_params:
            func_signature.append(sig)
        for _, sig in optional_params:
            func_signature.append(sig)
            
        return (func_signature, call_params)

    @classmethod
    def create_unknown_event_handler(cls, method_name: str, signature: inspect.Signature):
        func_signature, call_params = cls.create_signatures_and_params(signature)

        event_handler_str = textwrap.dedent(f"""
        def {method_name}({", ".join(func_signature)}):
            self.publish_unknown_event('{method_name}', {", ".join(call_params)})
        """)
        exec(event_handler_str)
        return t.cast(t.Callable[[t.Any], t.Any], locals()[method_name])

    def __init__(self, log_override: logging.Logger | None = None, **kwargs) -> None:
        # Ignorer les kwargs non supportés (verbosity, ignore_warnings, etc.)
        self._handlers: dict[str, ConsoleEventHandler] = {}
        self.logger = log_override or logger
        self.id = str(uuid.uuid4())
        self.logger.debug(f"SQLMeshEventConsole[{self.id}]: created")

    def publish_known_event(self, event_name: str, **kwargs: t.Any) -> None:
        console_event = get_console_event_by_name(event_name)
        assert console_event is not None, f"Event {event_name} not found"
        
        expected_kwargs_fields = console_event.__dataclass_fields__
        expected_kwargs: dict[str, t.Any] = {}
        unknown_args: dict[str, t.Any] = {}
        for key, value in kwargs.items():
            if key not in expected_kwargs_fields:
                unknown_args[key] = value
            else:
                expected_kwargs[key] = value
        
        event = console_event(**expected_kwargs, unknown_args=unknown_args)
        self.publish(event)

    def publish(self, event: ConsoleEvent) -> None:
        self.logger.debug(
            f"SQLMeshEventConsole[{self.id}]: sending event {event.__class__.__name__} to {len(self._handlers)} handlers"
        )
        for handler in self._handlers.values():
            handler(event)

    def publish_unknown_event(self, event_name: str, **kwargs: t.Any) -> None:
        self.logger.debug(
            f"SQLMeshEventConsole[{self.id}]: sending unknown '{event_name}' event to {len(self._handlers)} handlers"
        )
        self.logger.debug(f"SQLMeshEventConsole[{self.id}]: unknown event {event_name} {kwargs}")

    def add_handler(self, handler: ConsoleEventHandler) -> str:
        handler_id = str(uuid.uuid4())
        self.logger.debug(f"SQLMeshEventConsole[{self.id}]: Adding handler {handler_id}")
        self._handlers[handler_id] = handler
        return handler_id

    def remove_handler(self, handler_id: str) -> None:
        del self._handlers[handler_id]

# =============================================================================
# CONSOLE PERSONNALISÉE POUR CAPTURER LES AUDITS
# =============================================================================

class SQLMeshEventCaptureConsole(IntrospectingConsole):
    """
    Console SQLMesh personnalisée qui capture TOUS les événements :
    - Plan (création, application)
    - Apply (évaluation, promotion)
    - Audits (résultats, erreurs)
    - Debug (logs, erreurs, succès)
    """

    def __init__(self, translator=None, **kwargs):
        super().__init__(**kwargs)
        self._translator = translator  # ← Ajouter le translator
        self.audit_results: list[dict[str, t.Any]] = []
        self.audit_stats: dict[str, dict[str, int]] = {}
        self.plan_events: list[dict[str, t.Any]] = []
        self.evaluation_events: list[dict[str, t.Any]] = []
        self.log_events: list[dict[str, t.Any]] = []
        
        # Logger contextuel qui peut être changé dynamiquement
        # Récupérer le log_override depuis les kwargs ou utiliser un logger par défaut
        self._context_logger = kwargs.get('log_override') or logging.getLogger(__name__)
        # S'assurer que le logger est en niveau INFO
        self._context_logger.setLevel(logging.INFO)
        
        # Console initialisée et prête
        
        # Ajouter notre handler personnalisé
        self.add_handler(self._event_handler)
    
    @property
    def context_logger(self):
        """Retourne le logger contextuel actuel"""
        return self._context_logger
    
    @context_logger.setter
    def context_logger(self, logger):
        """Permet de changer le logger contextuel dynamiquement"""
        self._context_logger = logger

    def _event_handler(self, event: ConsoleEvent) -> None:
        """Handler principal qui capture TOUS les événements SQLMesh"""
        
        # Debug: afficher tous les événements reçus
        self.context_logger.debug(f"🔍 EVENT RECEIVED: {event.__class__.__name__}")
        
        # Capture des événements de plan
        if isinstance(event, StartPlanEvaluation):
            self._handle_start_plan_evaluation(event)
        elif isinstance(event, StopPlanEvaluation):
            self._handle_stop_plan_evaluation(event)
        
        # Capture des événements d'évaluation (où les audits se déclenchent)
        elif isinstance(event, StartSnapshotEvaluationProgress):
            self._handle_start_snapshot_evaluation(event)
        elif isinstance(event, UpdateSnapshotEvaluationProgress):
            self._handle_update_snapshot_evaluation(event)
        elif isinstance(event, StopEvaluationProgress):
            self._handle_stop_evaluation(event)
        
        # Capture des logs d'erreur (pour les audits qui échouent)
        elif isinstance(event, LogError):
            self._handle_log_error(event)
        elif isinstance(event, LogFailedModels):
            self._handle_log_failed_models(event)
        
        # Capture des logs de succès
        elif isinstance(event, LogSuccess):
            self._handle_log_success(event)
        
        # Capture des logs de statut
        elif isinstance(event, LogStatusUpdate):
            self._handle_log_status_update(event)
        


    def _handle_log_status_update(self, event: LogStatusUpdate) -> None:
        """Capture les logs de statut"""
        # Utiliser le logger Dagster si disponible
        if hasattr(self, '_dagster_logger') and self._dagster_logger:
            self._dagster_logger.info(f"ℹ️ SQLMesh: {event.message}")
        

    def _handle_start_plan_evaluation(self, event: StartPlanEvaluation) -> None:
        """Capture le début d'un plan"""
        plan_info = {
            'event_type': 'start_plan_evaluation',
            'plan_id': getattr(event.plan, 'plan_id', 'N/A'),
            'timestamp': t.cast(float, t.Any),
        }
        self.plan_events.append(plan_info)

    def _handle_stop_plan_evaluation(self, event: StopPlanEvaluation) -> None:
        """Capture la fin d'un plan"""
        plan_info = {
            'event_type': 'stop_plan_evaluation',
            'timestamp': t.cast(float, t.Any),
        }
        self.plan_events.append(plan_info)

    def _handle_start_snapshot_evaluation(self, event: StartSnapshotEvaluationProgress) -> None:
        """Capture le début de l'évaluation d'un snapshot (où les audits peuvent se déclencher)"""
        eval_info = {
            'event_type': 'start_snapshot_evaluation',
            'snapshot_name': event.snapshot.name,
            'snapshot_id': str(event.snapshot.snapshot_id),
            'timestamp': t.cast(float, t.Any),
        }
        self.evaluation_events.append(eval_info)

    def _handle_update_snapshot_evaluation(self, event: UpdateSnapshotEvaluationProgress) -> None:
        """Capture les mises à jour pendant l'évaluation (c'est ici que les audits se déclenchent !)"""
        self.context_logger.debug(f"✅ _handle_update_snapshot_evaluation called")
        eval_info = {
            'event_type': 'update_snapshot_evaluation',
            'snapshot_name': event.snapshot.name,
            'batch_idx': event.batch_idx,
            'duration_ms': event.duration_ms,
            'num_audits_passed': event.num_audits_passed,
            'num_audits_failed': event.num_audits_failed,
        }
        self.evaluation_events.append(eval_info)
        
        # Capture des résultats d'audit via les paramètres
        if event.num_audits_passed is not None or event.num_audits_failed is not None:
            if hasattr(self, '_dagster_logger') and self._dagster_logger:
                self._dagster_logger.info(f"✅ AUDITS RESULTS: {event.num_audits_passed} passed, {event.num_audits_failed} failed")
            
            # Si on a des audits dans ce snapshot, on peut les capturer ici
            if hasattr(event.snapshot, 'model') and hasattr(event.snapshot.model, 'audits_with_args') and event.snapshot.model.audits_with_args:
                audit_results = []
                for audit_obj, audit_args in event.snapshot.model.audits_with_args:
                    try:
                        # Utiliser le translator existant pour obtenir l'asset_key
                        asset_key = self._translator.get_asset_key(event.snapshot.model) if self._translator else None
                        
                        audit_result = {
                            'model_name': event.snapshot.model.name,
                            'asset_key': asset_key,
                            'audit_details': self._extract_audit_details(audit_obj, audit_args, event.snapshot.model),
                            'batch_idx': event.batch_idx,
                        }
                        audit_results.append(audit_result)
                    except Exception as e:
                        self._dagster_logger.warning(f"⚠️ Erreur lors de la capture d'audit: {e}")
                        continue
                
                self.audit_results.extend(audit_results)

    def _extract_audit_details(self, audit_obj, audit_args, model):
        """Extrait toutes les informations utiles d'un audit"""
        
        # Utiliser la fonction utilitaire
        sql_query = safe_extract_audit_query(
            model=model,
            audit_obj=audit_obj,
            audit_args=audit_args,
            logger=self._dagster_logger if hasattr(self, '_dagster_logger') else None
        )
        
        return {
            'name': getattr(audit_obj, 'name', 'unknown'),
            'sql': sql_query,
            'blocking': getattr(audit_obj, 'blocking', False),
            'skip': getattr(audit_obj, 'skip', False),
            'arguments': audit_args
        }

    def _handle_stop_evaluation(self, event: StopEvaluationProgress) -> None:
        """Capture la fin de l'évaluation"""
        eval_info = {
            'event_type': 'stop_evaluation',
            'success': event.success,
            'timestamp': t.cast(float, t.Any),
        }
        self.evaluation_events.append(eval_info)

    def _handle_log_error(self, event: LogError) -> None:
        """Capture les erreurs (y compris les audits qui échouent)"""
        error_info = {
            'event_type': 'log_error',
            'message': event.message,
            'timestamp': t.cast(float, t.Any),
        }
        self.log_events.append(error_info)

    def _handle_log_failed_models(self, event: LogFailedModels) -> None:
        """Capture les modèles qui ont échoué"""
        for error in event.errors:
            error_info = {
                'event_type': 'log_failed_model',
                'error': str(error),
                'timestamp': t.cast(float, t.Any),
            }
            self.log_events.append(error_info)

    def _handle_log_success(self, event: LogSuccess) -> None:
        """Capture les succès"""
        success_info = {
            'event_type': 'log_success',
            'message': event.message,
            'timestamp': t.cast(float, t.Any),
        }
        self.log_events.append(success_info)

    def get_audit_results(self) -> list[dict[str, t.Any]]:
        """Retourne tous les résultats d'audit capturés"""
        return self.audit_results

    def get_evaluation_events(self) -> list[dict[str, t.Any]]:
        """Retourne tous les événements d'évaluation"""
        return self.evaluation_events

    def get_plan_events(self) -> list[dict[str, t.Any]]:
        """Retourne tous les événements de plan"""
        return self.plan_events

    def get_all_events(self) -> dict[str, list[dict[str, t.Any]]]:
        """Retourne TOUS les événements capturés organisés par catégorie"""
        return {
            'audit_results': self.audit_results,
            'evaluation_events': self.evaluation_events,
            'plan_events': self.plan_events,
            'log_events': self.log_events,
        }

    def clear_events(self) -> None:
        """Nettoie tous les événements capturés"""
        self.audit_results.clear()
        self.audit_stats.clear()
        self.plan_events.clear()
        self.evaluation_events.clear()
        self.log_events.clear() 

 