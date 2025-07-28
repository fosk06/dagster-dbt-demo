# SQLMesh-Dagster Integration

Ce module fournit une intégration entre SQLMesh et Dagster, permettant de matérialiser des modèles SQLMesh comme des assets Dagster.

## Fonctionnalités

- **Matérialisation automatique** : Les modèles SQLMesh sont automatiquement matérialisés comme des assets Dagster
- **Support des external assets** : Les sources SQLMesh (external assets) sont mappées vers des AssetKeys Dagster
- **Translator extensible** : Système de translator personnalisable pour mapper les concepts SQLMesh vers Dagster
- **Singleton strict** : Une seule instance SQLMesh active à la fois
- **Multithreading avec AnyIO** : Exécution asynchrone pour éviter le blocage de Dagster
- **Caching intelligent** : Cache des contextes, modèles et translators pour les performances

## Utilisation de base

```python
from dagster import Definitions, RetryPolicy
from .decorators import sqlmesh_assets_factory
from .resource import SQLMeshResource

# Configuration du resource SQLMesh
sqlmesh_resource = SQLMeshResource(
    project_dir="sqlmesh_project",
    gateway="postgres",
    allow_breaking_changes=True,
)

# Configuration des assets SQLMesh
sqlmesh_assets = sqlmesh_assets_factory(
    sqlmesh_resource=sqlmesh_resource,
    name="sqlmesh_multi_asset",
    group_name="sqlmesh",
    op_tags={"team": "data", "env": "prod"},
    retry_policy=RetryPolicy(max_retries=2, delay=1.0),
)

defs = Definitions(
    assets=[sqlmesh_assets],
    resources={"sqlmesh": sqlmesh_resource},
)
```

## Translator personnalisé

Pour mapper les external assets (sources SQLMesh) vers vos conventions Dagster, vous pouvez créer un translator personnalisé :

```python
from .translator import SQLMeshTranslator
import dagster as dg

class MyCustomTranslator(SQLMeshTranslator):
    def get_external_asset_key(self, external_fqn: str) -> dg.AssetKey:
        """
        Mapping custom pour les external assets.
        Exemple: 'jaffle_db.main.raw_source_customers' → ['target', 'main', 'raw_source_customers']
        """
        parts = external_fqn.replace('"', '').split('.')
        # On ignore le catalog (jaffle_db), on prend le reste
        return dg.AssetKey(['target'] + parts[1:])

    def get_group_name(self, context, model) -> str:
        """
        Mapping custom pour les groupes.
        """
        model_name = getattr(model, "view_name", "")
        if model_name.startswith("stg_"):
            return "staging"
        elif model_name.startswith("mart_"):
            return "marts"
        return super().get_group_name(context, model)

# Utilisation avec le translator custom
sqlmesh_resource = SQLMeshResource(
    project_dir="sqlmesh_project",
    gateway="postgres",
    allow_breaking_changes=True,
    translator=MyCustomTranslator(),  # <--- Translator custom ici !
)
```

## Méthodes du translator

Le `SQLMeshTranslator` expose plusieurs méthodes que vous pouvez override :

### `get_external_asset_key(external_fqn: str) -> AssetKey`

Mappe un FQN d'external asset vers un AssetKey Dagster.

### `get_asset_key(model) -> AssetKey`

Mappe un modèle SQLMesh vers un AssetKey Dagster.

### `get_group_name(context, model) -> str`

Détermine le groupe pour un modèle.

### `get_tags(context, model) -> dict`

Génère les tags pour un modèle.

### `get_metadata(model, keys: list[str]) -> dict`

Extrait les métadonnées spécifiées du modèle.

## Exemple complet avec external assets

```python
from dagster import Definitions, RetryPolicy
from .decorators import sqlmesh_assets_factory
from .resource import SQLMeshResource
from .translator import SQLMeshTranslator

class SlingTranslator(SQLMeshTranslator):
    def get_external_asset_key(self, external_fqn: str) -> dg.AssetKey:
        """
        Mappe les sources SQLMesh vers les assets Sling.
        Format SQLMesh: 'jaffle_db.main.raw_source_customers'
        Format Sling: ['sling', 'raw_source_customers']
        """
        parts = external_fqn.replace('"', '').split('.')
        if len(parts) >= 3:
            catalog, schema, table = parts[0], parts[1], parts[2]
            if catalog == "jaffle_db" and schema == "main":
                return dg.AssetKey(["sling", table])
        # Fallback
        return dg.AssetKey(["external"] + parts[1:])

# Configuration avec translator custom
sqlmesh_resource = SQLMeshResource(
    project_dir="sqlmesh_project",
    gateway="postgres",
    allow_breaking_changes=True,
    translator=SlingTranslator(),
)

sqlmesh_assets = sqlmesh_assets_factory(
    sqlmesh_resource=sqlmesh_resource,
    name="sqlmesh_assets",
    group_name="sqlmesh",
)

defs = Definitions(
    assets=[sqlmesh_assets],
    resources={"sqlmesh": sqlmesh_resource},
)
```

## Architecture

### SQLMeshResource

- Gère le contexte SQLMesh et le caching
- Implémente le pattern singleton strict
- Utilise AnyIO pour le multithreading
- Accepte un translator personnalisé

### SQLMeshTranslator

- Mappe les concepts SQLMesh vers Dagster
- Extensible via l'héritage
- Gère les external assets et les dépendances

### sqlmesh_assets_factory

- Génère dynamiquement les AssetSpec pour tous les modèles SQLMesh
- Utilise le translator pour mapper les dépendances
- Valide les external dependencies

## Performance

- **Singleton strict** : Une seule instance SQLMesh active
- **Caching** : Contextes, modèles et translators mis en cache
- **Multithreading** : Utilise AnyIO pour éviter le blocage de Dagster
- **Lazy loading** : Les ressources sont chargées à la demande

## Migration depuis l'ancien système

Si vous utilisiez l'ancien système avec `external_asset_key_mapper` :

```python
# Ancien système (callback)
sqlmesh_resource = SQLMeshResource(
    project_dir="sqlmesh_project",
    external_asset_key_mapper=my_mapper_function,
)

# Nouveau système (translator)
class MyTranslator(SQLMeshTranslator):
    def get_external_asset_key(self, external_fqn: str) -> dg.AssetKey:
        return my_mapper_function(external_fqn)

sqlmesh_resource = SQLMeshResource(
    project_dir="sqlmesh_project",
    translator=MyTranslator(),
)
```
