# SQLMesh Dagster Assets Integration

This module provides a minimalist and maintainable integration between SQLMesh and Dagster for asset declaration and materialization.

## Quick Usage

Declare all your SQLMesh assets in Dagster in just a few lines:

```python
from dagster import Definitions
from .decorators import sqlmesh_assets_factory
from .resource import SQLMeshResource

sqlmesh_resource = SQLMeshResource(project_dir="sqlmesh_project", target="dev")
sqlmesh_assets = sqlmesh_assets_factory(
    sqlmesh_resource=sqlmesh_resource,
    name="sqlmesh_multi_asset",
    group_name="sqlmesh",
)

defs = Definitions(
    assets=[sqlmesh_assets],
    resources={
        "sqlmesh": sqlmesh_resource,
    },
)
```

## Key Points

- **No manual asset function to write**: the factory handles everything (declaration, materialization, outputs, metadata).
- **All business logic** (selection, ordering, snapshot extraction, etc.) is centralized in the `SQLMeshResource`.
- **API inspired by dagster-dbt**: simple, factorized, maintainable.
- **Customizable**: change the name, group, or resource as needed.

## Going Further

- Add hooks, checks, or schedules on top of this base.
- Customize the resource for advanced needs (multi-environment, logging, etc.).

---

For any questions or further development, see the source code or contact the author of the refactor.
