import re
from sqlmesh.core.context import Context
import io
import sys
from datetime import datetime
from typing import Any, Callable, Dict, Tuple


def parse_model_name(model_name: str) -> list[str]:
    return re.findall(r'"([^"]+)"', model_name)


def capture_stdout(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Tuple[str, Any]:
    buffer = io.StringIO()
    sys_stdout = sys.stdout
    sys.stdout = buffer
    try:
        result = func(*args, **kwargs)
    finally:
        sys.stdout = sys_stdout
    return buffer.getvalue(), result


def init_state() -> Dict[str, Any]:
    return {
        "models": [],
        "modified_models": [],
        "environments": [],
        "diff": False,
        "diff_details": None,
        "info": None,
        "context": None,
        "plan": None
    }


def init_context(state: Dict[str, Any], paths: str = "sqlmesh_project") -> Dict[str, Any]:
    state["context"] = Context(paths=paths)
    return state


def init_plan(state: Dict[str, Any]) -> Dict[str, Any]:
    state["plan"] = state["context"].plan()
    return state


def apply_plan(state: Dict[str, Any]) -> Dict[str, Any]:
    state["context"].apply(state["plan"])
    return state


def get_models(state: Dict[str, Any]) -> Dict[str, Any]:
    # On parse chaque nom de modèle
    state["models"] = [parse_model_name(name) for name in list(state["context"].models.keys())]
    return state


def get_modified_models(state: Dict[str, Any]) -> Dict[str, Any]:
    # On parse chaque nom de modèle modifié
    state["modified_models"] = [parse_model_name(name) for name in list(state["plan"].context_diff.modified_snapshots)]
    return state


def get_environments(state: Dict[str, Any]) -> Dict[str, Any]:
    envs = state["context"].state_reader.get_environments()
    state["environments"] = [f"{env.name} (expires: {getattr(env, 'expiration_ts', 'N/A')})" for env in envs]
    return state


def get_diff(state: Dict[str, Any], environment: str = "prod", detailed: bool = True) -> Dict[str, Any]:
    output, has_changes = capture_stdout(state["context"].diff, environment=environment, detailed=detailed)
    state["diff"] = has_changes
    state["diff_details"] = output
    return state


def get_context_info(state: Dict[str, Any]) -> Dict[str, Any]:
    info = {
        "models_count": len(state["context"].models),
        "macros_count": len(getattr(state["context"], '_macros', {})),
        "connection_ok": False
    }
    try:
        state["context"].engine_adapter.ping()
        info["connection_ok"] = True
    except Exception:
        info["connection_ok"] = False
    state["info"] = info
    return state


def pipeline(state: Dict[str, Any], *funcs: Callable[[Dict[str, Any]], Dict[str, Any]]) -> Dict[str, Any]:
    for func in funcs:
        state = func(state)
    return state


def print_sqlmesh_state(state: Dict[str, Any]) -> None:
    print("\nState summary:")
    print("Models:", ["/".join(m) for m in state.get("models", [])])
    print("Models modified in the plan:", ["/".join(m) for m in state.get("modified_models", [])])
    print("Existing environments:")
    for env in state.get("environments", []):
        print(env)
    print("Differences with prod:", "Yes" if state.get("diff") else "No")
    if state.get("diff_details"):
        print("Diff details:\n", state["diff_details"])
    print("Context information:")
    for k, v in state.get("info", {}).items():
        print(f"{k}: {v}")
    print("Last materializations:")
    for model, ts in state.get("last_materializations", {}).items():
        model_str = "/".join(model) if isinstance(model, (list, tuple)) else str(model)
        if ts:
            print(f"{model_str}: raw ts = {ts}")
            dt = datetime.utcfromtimestamp(ts / 1000)
            print(f"{model_str}: {dt} (UTC)")
        else:
            print(f"{model_str}: never materialized")
    print("Last snapshot updates:")
    for model, ts in state.get("last_snapshot_dates", {}).items():
        model_str = "/".join(model) if isinstance(model, (list, tuple)) else str(model)
        if ts:
            print(f"{model_str}: snapshot updated_ts = {ts}")
            dt = datetime.utcfromtimestamp(ts / 1000)
            print(f"{model_str}: {dt} (UTC)")
        else:
            print(f"{model_str}: never updated")

def get_last_materialization_dates(state: Dict[str, Any], environment: str = "prod") -> Dict[str, Any]:
    context = state["context"]
    envs = context.state_reader.get_environments()
    env = next((e for e in envs if e.name == environment), None)
    if not env:
        state["last_materializations"] = {}
        state["last_snapshot_dates"] = {}
        return state

    snapshots = context.state_reader.get_snapshots(env.snapshots)
    last_materializations = {}
    last_snapshot_dates = {}
    for name, snapshot in snapshots.items():
        model_name = getattr(snapshot, "model_name", None)
        if not model_name and hasattr(snapshot, "model"):
            model_name = getattr(snapshot.model, "name", None)
        if not model_name:
            model_name = str(name)
        parsed_name = parse_model_name(model_name)
        intervals = getattr(snapshot, "intervals", [])
        if intervals:
            last_end = max(end for start, end in intervals)
            last_materializations[tuple(parsed_name)] = last_end
        else:
            last_materializations[tuple(parsed_name)] = None
        # Ajout de la date de snapshot (updated_ts)
        updated_ts = getattr(snapshot, "updated_ts", None)
        if updated_ts:
            last_snapshot_dates[tuple(parsed_name)] = updated_ts
        else:
            last_snapshot_dates[tuple(parsed_name)] = None
    state["last_materializations"] = last_materializations
    state["last_snapshot_dates"] = last_snapshot_dates
    return state

def create_sqlmesh_state(paths: str = "sqlmesh_project") -> Dict[str, Any]:
    return pipeline(
        init_state(),
        lambda s: init_context(s, paths=paths),
        init_plan,
        apply_plan,
        get_models,
        get_modified_models,
        get_environments,
        get_diff,
        get_context_info,
        get_last_materialization_dates
    ) 


if __name__ == "__main__":
    state = create_sqlmesh_state()
    print_sqlmesh_state(state)