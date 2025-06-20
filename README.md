# Jaffle Platform

Ce projet est une démonstration d'une plateforme de données moderne utilisant :

- [Dagster](https://dagster.io/) pour l'orchestration
- [DuckDB](https://duckdb.org/) pour le stockage et le traitement des données
- [DBT](https://www.getdbt.com/) pour la transformation des données

## Installation

### Prérequis

- Python 3.11+
- [uv](https://github.com/astral-sh/uv) pour la gestion des dépendances

### Installation des dépendances

```bash
# Installation de uv (si pas déjà fait)
pip install uv

# Synchronisation des dépendances avec uv
uv sync

# En cas de problème avec uv, vous pouvez forcer la synchronisation
uv pip install -r requirements.txt --force-reinstall
```

Si vous rencontrez des problèmes avec `uv sync`, vous pouvez aussi utiliser `pip` :

```bash
pip install -r requirements.txt
```

## À propos du projet

Le projet simule une chaîne de restaurants "Jaffle Shop" (un jaffle est un sandwich grillé australien). Il trace les commandes, les clients, les produits et les approvisionnements de cette chaîne de restaurants.

### Structure des données

#### Sources (schéma `raw`)

- `raw_customers` : Les clients de la chaîne

  - `id` : Identifiant unique du client
  - `name` : Nom du client

- `raw_orders` : Les commandes passées

  - `id` : Identifiant unique de la commande
  - `customer_id` : ID du client qui a passé la commande
  - `ordered_at` : Date et heure de la commande
  - `store_id` : ID du restaurant
  - `status` : Statut de la commande (completed)

- `raw_items` : Les items des commandes

  - `id` : Identifiant unique de l'item
  - `order_id` : ID de la commande
  - `product_id` : ID du produit commandé

- `raw_products` : Les produits disponibles

  - `sku` : Identifiant unique du produit
  - `name` : Nom du produit
  - `type` : Type de produit (jaffle/beverage)
  - `price` : Prix de vente

- `raw_stores` : Les restaurants

  - `id` : Identifiant unique du restaurant
  - `name` : Nom du restaurant
  - `opened_at` : Date d'ouverture
  - `tax_rate` : Taux de taxe applicable

- `raw_supplies` : Les approvisionnements
  - `id` : Identifiant unique de l'approvisionnement
  - `name` : Nom de l'approvisionnement
  - `product_id` : ID du produit concerné
  - `supply_cost` : Coût d'approvisionnement
  - `perishable` : Si l'approvisionnement est périssable

#### Modèles transformés

##### Staging (vues)

- `stg_customers` : Nettoyage des données clients
- `stg_orders` : Nettoyage des données commandes
- `stg_order_items` : Nettoyage des données items
- `stg_products` : Nettoyage des données produits
- `stg_stores` : Nettoyage des données restaurants
- `stg_supplies` : Nettoyage des données approvisionnements

##### Marts (tables)

- `customers` : Vue enrichie des clients

  - Informations de base du client
  - Statistiques sur les commandes (nombre, valeur moyenne)
  - Statistiques sur les produits (nourriture vs boissons)
  - Montants totaux (revenus, coûts)

- `orders` : Vue enrichie des commandes

  - Informations détaillées sur chaque commande
  - Totaux (items, revenus, coûts, profit)
  - Liens vers les clients et restaurants

- `order_items` : Vue enrichie des items

  - Détails du produit
  - Prix de vente et coût d'approvisionnement
  - Profit par item

- `products` : Vue enrichie des produits

  - Informations de base du produit
  - Type de produit (nourriture/boisson)
  - Prix de vente

- `supplies` : Vue enrichie des approvisionnements
  - Informations de base de l'approvisionnement
  - Coût et périssabilité
  - Lien vers le produit

## Architecture technique

1. **Ingestion** : Les données sont chargées dans DuckDB via Sling (Dagster)
2. **Transformation** : DBT est utilisé pour créer les modèles analytiques
3. **Orchestration** : Dagster orchestre l'ensemble du pipeline avec un schedule quotidien

## Tests

Le projet inclut 85 tests DBT :

- Tests d'unicité des clés primaires
- Tests de non-nullité des champs requis
- Tests de relations entre les tables
- Tests de plages de valeurs (montants positifs)
- Tests de valeurs acceptées (types de produits, statuts de commandes)

## Configuration

Le projet utilise les configurations suivantes :

- Les modèles staging sont matérialisés en vues
- Les modèles marts sont matérialisés en tables
- Le schéma par défaut est `main`
- Le fuseau horaire est configuré pour l'Europe/Paris

## Interrogation des données

Pour interroger les données avec DuckDB, vous avez deux options :

### 1. Mode interactif

```bash
# Depuis le dossier dbt/jdbt
duckdb jaffle_platform.duckdb
```

### 2. Mode ligne de commande (recommandé)

```bash
# Syntaxe générale
duckdb jaffle_platform.duckdb -c "VOTRE_REQUETE;"

# Exemples :

# Liste des schémas disponibles
duckdb jaffle_platform.duckdb -c "SHOW SCHEMAS;"

# Liste des tables et vues dans le schéma main
duckdb jaffle_platform.duckdb -c "SHOW TABLES IN main;"

# Top 10 des clients par montant total dépensé
duckdb jaffle_platform.duckdb -c "
SELECT
    customer_name,
    total_revenue,
    number_of_orders,
    avg_order_value
FROM main.customers
ORDER BY total_revenue DESC
LIMIT 10;"

# Ventes par type de produit et par mois
duckdb jaffle_platform.duckdb -c "
SELECT
    date_trunc('month', order_date) as month,
    product_type,
    count(*) as number_of_items,
    sum(product_price) as total_revenue,
    sum(gross_profit_per_item) as total_profit
FROM main.order_items
GROUP BY 1, 2
ORDER BY 1, 2;"

# Performance des restaurants
duckdb jaffle_platform.duckdb -c "
SELECT
    s.store_name,
    count(DISTINCT o.order_id) as number_of_orders,
    sum(o.total_revenue) as total_revenue,
    sum(o.total_profit) as total_profit,
    avg(o.total_items_count) as avg_items_per_order
FROM main.orders o
JOIN main.stg_stores s ON o.store_id = s.store_id
GROUP BY 1
ORDER BY total_revenue DESC;"

# Produits les plus rentables
duckdb jaffle_platform.duckdb -c "
SELECT
    product_name,
    product_type,
    count(*) as times_ordered,
    avg(gross_profit_per_item) as avg_profit_per_item,
    sum(gross_profit_per_item) as total_profit
FROM main.order_items
GROUP BY 1, 2
ORDER BY total_profit DESC
LIMIT 10;"
```

### Export des données

```bash
# Export en CSV
duckdb jaffle_platform.duckdb -c "COPY (SELECT * FROM main.customers) TO 'customers.csv';"

# Export en Parquet
duckdb jaffle_platform.duckdb -c "COPY (SELECT * FROM main.orders) TO 'orders.parquet' (FORMAT PARQUET);"

# Export en JSON
duckdb jaffle_platform.duckdb -c "COPY (SELECT * FROM main.products) TO 'products.json' (FORMAT JSON);"
```

### Astuces DuckDB

En mode interactif :

- `.tables` pour lister toutes les tables
- `.schema TABLE_NAME` pour voir la structure d'une table
- `.mode markdown` pour un affichage en markdown
- `.headers on` pour afficher les en-têtes de colonnes
- `.quit` pour quitter

En ligne de commande :

```bash
# Lister les tables
duckdb jaffle_platform.duckdb -c ".tables"

# Voir le schéma d'une table
duckdb jaffle_platform.duckdb -c ".schema main.customers"

# Activer les en-têtes et utiliser le mode markdown
duckdb jaffle_platform.duckdb -c ".mode markdown" -c ".headers on" -c "SELECT * FROM main.customers LIMIT 5;"
```

## Cheatsheet

### Scaffolding an Asset Check

To create a new asset check for an existing asset, you can use the `dagster scaffold` command. This will generate a boilerplate file for your check.

For example, to create a check for the `product_sentiment_scores` asset:

```bash
dg scaffold defs dagster.asset_check --format=python --asset-key product_sentiment_scores assets_checks/product_sentiment_scores.py
```
