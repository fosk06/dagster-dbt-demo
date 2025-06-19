# Jaffle Platform

Ce projet est une démonstration d'une plateforme de données moderne utilisant :

- [Dagster](https://dagster.io/) pour l'orchestration
- [DuckDB](https://duckdb.org/) pour le stockage et le traitement des données
- [DBT](https://www.getdbt.com/) pour la transformation des données

## À propos du projet

Le projet simule une chaîne de restaurants "Jaffle Shop" (un jaffle est un sandwich grillé australien). Il trace les commandes et les clients de cette chaîne de restaurants.

### Structure des données

#### Sources (schéma `jaffle-data`)

- `raw_customers` : Les clients de la chaîne

  - `id` : Identifiant unique du client
  - `name` : Nom du client

- `raw_orders` : Les commandes passées
  - `id` : Identifiant unique de la commande
  - `customer` : ID du client qui a passé la commande
  - `ordered_at` : Date et heure de la commande
  - `store_id` : ID du restaurant
  - `subtotal` : Montant avant taxes
  - `tax_paid` : Taxes payées
  - `order_total` : Montant total de la commande

#### Modèles transformés

- `customers` : Vue enrichie des clients avec leur historique de commandes

  - Informations de base du client
  - Première et dernière commande
  - Nombre total de commandes
  - Montant total dépensé

- `orders` : Vue enrichie des commandes
  - Informations détaillées sur chaque commande
  - Liens vers les clients

## Génération des données

Les données sources sont générées aléatoirement grâce au package `jaffle-shop-generator`. Pour générer un nouveau jeu de données :

```bash
jafgen 3 --pre raw
```

Cette commande va générer 3 jours de données dans le schéma `jaffle-data`.

## Architecture technique

1. **Ingestion** : Les données sont chargées dans DuckDB via Dagster
2. **Transformation** : DBT est utilisé pour créer les modèles analytiques
3. **Orchestration** : Dagster orchestre l'ensemble du pipeline et expose les tests DBT comme des "asset checks"

## Tests

Le projet inclut plusieurs types de tests DBT :

- Tests d'unicité des clés primaires
- Tests de non-nullité des champs requis
- Tests de relations entre les tables
- Tests de plages de valeurs (montants positifs)
- Tests de valeurs acceptées (statuts de commandes)
