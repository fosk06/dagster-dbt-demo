# SQLMesh-Dagster Integration

This module provides a complete integration between SQLMesh and Dagster, allowing SQLMesh models to be materialized as Dagster assets with support for audits, metadata, and adaptive scheduling.

## Features

### 🎯 **SQLMesh Model to Dagster Asset Conversion**

- **Automatic materialization** : SQLMesh models are automatically converted to Dagster assets
- **External assets support** : SQLMesh sources (external assets) are mapped to Dagster AssetKeys
- **Automatic dependencies** : Dependencies between models are preserved in Dagster
- **Partitioning** : Support for partitioned SQLMesh models (managed by SQLMesh, no integration with Dagster partitions - no Dagster → SQLMesh backfill)

### 📊 **SQLMesh Metadata Integration to Dagster**

- **Complete metadata** : Cron, tags, kind, dialect, query, partitioned_by, clustered_by
- **Code versioning** : Uses SQLMesh data_hash for Dagster versioning
- **Column descriptions** : Table metadata with descriptions
- **Customizable tags** : SQLMesh tags mapping to Dagster

### ✅ **SQLMesh Audits to Asset Checks Conversion**

- **Automatic audits** : SQLMesh audits become Dagster AssetCheckSpec
- **AssetCheckResult** : Automatic emission of audit results
- **Audit metadata** : SQL query, arguments, dialect, blocking status
- **Non-blocking** : Dagster checks are non-blocking (SQLMesh handles blocking)

### ⏰ **Adaptive Scheduling**

- **Automatic analysis** : Detection of the finest granularity from SQLMesh crons
- **Adaptive schedule** : Automatic creation of a Dagster schedule based on crons
- **Intelligent execution** : SQLMesh manages which models should be executed
- **Monitoring** : Detailed logs and granularity metadata

### 🔧 **All-in-One Factory**

- **Simple configuration** : Single function to configure everything
- **Extensible translator** : Customizable translator system
- **Automatic validation** : External dependencies validation
- **Retry policy** : Centralized retry policy configuration

## Basic Usage

### **Simple Factory (Recommended)**

```python
from dagster import RetryPolicy, AssetKey, Backoff
from .decorators import sqlmesh_definitions_factory
from .translator import SQLMeshTranslator

class SlingToSqlmeshTranslator(SQLMeshTranslator):
    def get_external_asset_key(self, external_fqn: str) -> AssetKey:
        """
        Custom mapping for external assets.
        SQLMesh: 'jaffle_db.main.raw_source_customers' → Sling: ['target', 'main', 'raw_source_customers']
        """
        parts = external_fqn.replace('"', '').split('.')
        if len(parts) >= 3:
            catalog, schema, table = parts[0], parts[1], parts[2]
            return AssetKey(['target', 'main', table])
        return AssetKey(['external'] + parts[1:])

# All-in-one factory: everything configured in one line!
defs = sqlmesh_definitions_factory(
    project_dir="sqlmesh_project",
    gateway="postgres",
    translator=SlingToSqlmeshTranslator(),
    concurrency_limit=1,
    ignore_cron=True,  # only for testing purposes
    name="sqlmesh_multi_asset",
    group_name="sqlmesh",
    op_tags={"team": "data", "env": "prod"},
    retry_policy=RetryPolicy(max_retries=1, delay=30.0, backoff=Backoff.EXPONENTIAL),
)
```

### **Advanced Configuration**

```python
from dagster import Definitions, RetryPolicy
from .decorators import sqlmesh_assets_factory, sqlmesh_adaptive_schedule_factory
from .resource import SQLMeshResource
from .translator import SQLMeshTranslator

# SQLMesh resource configuration
sqlmesh_resource = SQLMeshResource(
    project_dir="sqlmesh_project",
    gateway="postgres",
    translator=SlingToSqlmeshTranslator(),
    concurrency_limit=1,
    ignore_cron=True  # only for testing purposes
)

# SQLMesh assets configuration
sqlmesh_assets = sqlmesh_assets_factory(
    sqlmesh_resource=sqlmesh_resource,
    name="sqlmesh_multi_asset",
    group_name="sqlmesh",
    op_tags={"team": "data", "env": "prod"},
    retry_policy=RetryPolicy(max_retries=1, delay=30.0, backoff=Backoff.EXPONENTIAL),
)

# Adaptive schedule and job created automatically
sqlmesh_adaptive_schedule, sqlmesh_job, _ = sqlmesh_adaptive_schedule_factory(
    sqlmesh_resource=sqlmesh_resource
)

defs = Definitions(
    assets=[sqlmesh_assets],
    jobs=[sqlmesh_job],
    schedules=[sqlmesh_adaptive_schedule],
    resources={
        "sqlmesh": sqlmesh_resource,
    },
)
```

## Custom Translator

To map external assets (SQLMesh sources) to your Dagster conventions, you can create a custom translator:

```python
from .translator import SQLMeshTranslator
import dagster as dg

class MyCustomTranslator(SQLMeshTranslator):
    def get_external_asset_key(self, external_fqn: str) -> dg.AssetKey:
        """
        Custom mapping for external assets.
        Example: 'jaffle_db.main.raw_source_customers' → ['target', 'main', 'raw_source_customers']
        """
        parts = external_fqn.replace('"', '').split('.')
        # We ignore the catalog (jaffle_db), we take the rest
        return dg.AssetKey(['target'] + parts[1:])

    def get_group_name(self, context, model) -> str:
        """
        Custom mapping for groups.
        """
        model_name = getattr(model, "view_name", "")
        if model_name.startswith("stg_"):
            return "staging"
        elif model_name.startswith("mart_"):
            return "marts"
        return super().get_group_name(context, model)
```

## Translator Methods

The `SQLMeshTranslator` exposes several methods you can override:

### `get_external_asset_key(external_fqn: str) -> AssetKey`

Maps an external asset FQN to a Dagster AssetKey.

### `get_asset_key(model) -> AssetKey`

Maps a SQLMesh model to a Dagster AssetKey.

### `get_group_name(context, model) -> str`

Determines the group for a model.

### `get_tags(context, model) -> dict`

Generates tags for a model.

### `get_metadata(model, keys: list[str]) -> dict`

Extracts specified metadata from the model.

## Asset Checks and Audits

### **Automatic Audit Conversion**

SQLMesh audits are automatically converted to Dagster AssetCheckSpec:

```python
# SQLMesh audit
MODEL (
    name customers,
    audits (
        not_null(column=id),
        unique_values(columns=[id, email])
    )
);

# Automatically becomes in Dagster
AssetCheckSpec(
    name="not_null",
    asset=AssetKey(["customers"]),
    blocking=False,  # SQLMesh handles blocking
    description="SQLMesh audit: not_null(column=id)"
)
```

### **AssetCheckResult Emission**

During execution, audit results are emitted as AssetCheckResult:

```python
AssetCheckResult(
    passed=True,
    asset_key=AssetKey(["customers"]),
    check_name="not_null",
    metadata={
        "sqlmesh_model_name": "customers",
        "audit_query": "SELECT COUNT(*) FROM customers WHERE id IS NULL",
        "audit_blocking": False,
        "audit_dialect": "postgres",
        "audit_args": {"column": "id"}
    }
)
```

## Adaptive Scheduling

### **Automatic Cron Analysis**

The system automatically analyzes all SQLMesh crons and determines the finest granularity:

```python
# If you have models with different crons:
# - customers: @daily
# - orders: @hourly
# - events: */5 * * * * (every 5 minutes)

# The adaptive schedule will be: */5 * * * * (every 5 minutes)
```

### **Intelligent Execution**

The schedule runs `sqlmesh run` on all models, but SQLMesh automatically manages which models should be executed:

```python
# The schedule simply does:
sqlmesh_resource.context.run(
    ignore_cron=False,  # SQLMesh respects crons
    execution_time=datetime.datetime.now(),
)
```

## Architecture

### **SQLMeshResource**

- Manages SQLMesh context and caching
- Implements strict singleton pattern
- Uses AnyIO for multithreading
- Accepts a custom translator

### **SQLMeshTranslator**

- Maps SQLMesh concepts to Dagster
- Extensible via inheritance
- Handles external assets and dependencies

### **SQLMesh Metadata via Tags**

You can pass metadata from SQLMesh models to Dagster assets using the tag convention `dagster:property:value`:

```sql
-- In your SQLMesh model
MODEL (
    name customers,
    tags ARRAY["dagster:group_name:sqlmesh_datamarts"],
    -- ... other model properties
);
```

#### **Supported Properties**

Currently supported Dagster properties via tags:

- **`dagster:group_name:value`** : Sets the Dagster asset group name
  - Example: `"dagster:group_name:sqlmesh_datamarts"`
  - Result: Asset will be in the "sqlmesh_datamarts" group

#### **Tag Convention**

The convention follows the pattern: `dagster:property:value`

- **`dagster`** : Prefix to indicate this is for Dagster
- **`property`** : The Dagster property to update on the asset
- **`value`** : The value to set for that property

#### **Priority Order**

When determining asset properties, the translator follows this priority:

1. **SQLMesh tags** : `dagster:group_name:value` (highest priority)
2. **Factory parameter** : `group_name="sqlmesh"` in factory call
3. **Default logic** : Automatic group determination based on model path

### **sqlmesh_definitions_factory**

- All-in-one factory for simple configuration
- Automatically creates: resource, assets, job, schedule
- Validates external dependencies
- Returns Definitions directly

### **SQLMeshEventCaptureConsole**

- Custom SQLMesh console to capture events
- Captures audit results for AssetCheckResult
- Handles metadata serialization

## Performance

- **Strict singleton** : Only one active SQLMesh instance
- **Caching** : Contexts, models and translators are cached
- **Multithreading** : Uses AnyIO to avoid Dagster blocking
- **Lazy loading** : Resources are loaded on demand
- **Early validation** : External dependencies validation before execution

## Development Workflow

### **SQLMesh Development Philosophy**

This module follows SQLMesh's philosophy of **separation of concerns**:

- **Development** : Use SQLMesh CLI for development and schema changes
- **Production** : Use SQLMesh CLI for promoting changes
- **Orchestration** : Use this Dagster module only for running models

### **Development Workflow**

#### **1. Local Development**

```bash
# Develop your models locally
sqlmesh plan dev
sqlmesh apply dev

# Test your changes
sqlmesh run dev
```

#### **2. Production Promotion**

```bash
# Promote changes to production
sqlmesh plan prod
sqlmesh apply prod

# Or use CI/CD pipeline
# - sqlmesh plan prod
# - sqlmesh apply prod
```

#### **3. Dagster Orchestration**

```python
# Dagster takes over for production runs
# - Automatic scheduling via adaptive schedule
# - Manual runs via Dagster UI
# - Only executes: sqlmesh run prod
```

### **Module Responsibilities**

#### **What this module DOES:**

- ✅ **Orchestrates** `sqlmesh run` commands
- ✅ **Schedules** model execution
- ✅ **Monitors** execution and audits
- ✅ **Emits** Dagster events and metadata

#### **What this module DOES NOT:**

- ❌ **Plan changes** (`sqlmesh plan`)
- ❌ **Apply changes** (`sqlmesh apply`)
- ❌ **Handle breaking changes**
- ❌ **Manage environments**

### **Breaking Changes Management**

Breaking changes are handled **outside** this module:

- **Development** : `sqlmesh plan dev` + manual review
- **Production** : `sqlmesh plan prod` + CI/CD approval
- **Orchestration** : This module only runs approved models

### **Environment Separation**

```bash
# Development (SQLMesh CLI)
sqlmesh plan dev
sqlmesh apply dev
sqlmesh run dev

# Production (Dagster module)
# Automatically runs: sqlmesh run prod
# Based on schedules and triggers
```

This separation ensures:

- ✅ **Clear responsibilities** : Development vs Orchestration
- ✅ **Safe deployments** : Breaking changes handled by SQLMesh CLI
- ✅ **Reliable orchestration** : Dagster only runs approved models
- ✅ **CI/CD friendly** : Standard SQLMesh workflow for deployments

## Limitations

- **No Dagster → SQLMesh backfill** : Partitions managed only by SQLMesh itself (run a materialization to backfill)
- **Breaking changes** : Handled outside the module (SQLMesh CLI or CI/CD)
- **Environment management** : SQLMesh CLI or CI/CD

## Troubleshooting

### Common Issues

#### **"Invalid cron" errors**

- **Cause** : Cron faster than 5 minutes
- **Solution** : Use `ignore_cron=True` for testing

#### **External asset mapping errors**

- **Cause** : Translator doesn't handle FQN format
- **Solution** : Check `get_external_asset_key` method

#### **Performance issues**

- **Cause** : Too many models loaded
- **Solution** : Use `concurrency_limit` and caching
