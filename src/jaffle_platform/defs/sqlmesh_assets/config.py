from pydantic import Field
from typing import Any, Optional
from sqlmesh.core.config import Config as MeshConfig

class SQLMeshContextConfig:
    """
    Configuration object for SQLMesh context, inspired by dagster-sqlmesh.
    Allows specifying project directory, gateway, and config overrides.
    """
    def __init__(
        self,
        project_dir: str,
        gateway: Optional[str] = None,
        config_override: Optional[dict[str, Any]] = None,
    ):
        self.project_dir = project_dir
        self.gateway = gateway
        self.config_override = config_override

    @property
    def sqlmesh_config(self) -> Optional[MeshConfig]:
        if self.config_override:
            return MeshConfig.parse_obj(self.config_override)
        return None 