# DBT Sandbox – Architecture Médaillon Hive

Projet sandbox DBT conçu pour des pipelines de données sur **Apache Hive SQL**, avec une architecture médaillon (Bronze → Silver → Gold) et des mises à jour **incrémentielles partitionnées** par année/mois/jour de modification.

---

## Table des matières

- [Présentation](#présentation)
- [Architecture médaillon](#architecture-médaillon)
- [Mécanique incrémentielle](#mécanique-incrémentielle)
- [Structure du projet](#structure-du-projet)
- [Macros Jinja réutilisables](#macros-jinja-réutilisables)
- [Prérequis](#prérequis)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Tests](#tests)

---

## Présentation

Ce sandbox illustre les bonnes pratiques DBT pour un cluster Hive en production :

| Problème courant | Solution apportée |
|---|---|
| Full scan à chaque run | Filtrage sur partitions `annee/mois/jour` |
| Doublons à l'ingestion | Macro `deduplicate()` en Silver |
| Arrivées tardives | Fenêtre de lookback configurable (`incremental_lookback_days`) |
| Noms de schémas par environnement | Override `generate_schema_name()` → `dev_sandbox_bronze`, `prod_sandbox_gold` |
| Logique incrémentielle répétitive | Macro `incremental_partition_predicate()` centralisée |

Le projet s'appuie sur **dbt-hive** (adaptateur officiel ThriftServer) et utilise le format **Parquet** pour toutes les tables matérialisées.

---

## Architecture médaillon

```
Source brute (Hive raw)
        │
        ▼
┌──────────────────────────────────────────────────────┐
│  BRONZE  –  Ingestion brute                          │
│  Partitions : annee / mois / jour                    │
│  • Copie fidèle de la source                         │
│  • Ajout _loaded_at et _dbt_invocation_id            │
│  • Aucune transformation métier                      │
└──────────────────────┬───────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────┐
│  SILVER  –  Nettoyage & Enrichissement               │
│  Partitions : annee / mois / jour                    │
│  • Dédoublonnage (ROW_NUMBER sur clé + updated_at)   │
│  • Normalisation des types et des chaînes            │
│  • Jointure avec les référentiels (seeds)            │
│  • Masquage RGPD (email → hash@domaine)              │
│  • Rejet des lignes invalides                        │
└──────────────────────┬───────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────┐
│  GOLD  –  Agrégats métier                            │
│  Partitions : annee / mois                           │
│  • KPI journaliers : CA, panier moyen, annulations   │
│  • KPI clients : CA mensuel, fidélité                │
│  • Recalcul du mois entier pour cohérence agrégats   │
└──────────────────────────────────────────────────────┘
```

### Modèles par couche

| Couche | Modèle | Description |
|---|---|---|
| Bronze | `brz_commandes` | Flux de commandes brut |
| Bronze | `brz_clients` | Référentiel clients brut |
| Silver | `slv_commandes` | Commandes nettoyées et enrichies |
| Silver | `slv_clients` | Clients normalisés avec email masqué |
| Gold | `gld_commandes_par_jour` | CA, volume et taux d'annulation journaliers |
| Gold | `gld_kpi_clients` | KPI clients mensuels |

---

## Mécanique incrémentielle

### Stratégie `insert_overwrite` (Hive)

Hive ne supporte pas `MERGE`. La stratégie utilisée est **`insert_overwrite`** : DBT identifie les partitions impactées et les réécrit entièrement.

```
Run N-1                Run N (incrémentiel)
─────────────          ──────────────────────────────
annee=2024             annee=2024
  mois=01    ✓           mois=01    ✓  (inchangé)
  mois=02    ✓           mois=02    ✓  (inchangé)
                         mois=03    ✓  (nouvelles données → réécrit)
```

### Filtre de partition

La macro `incremental_partition_predicate()` génère automatiquement le bon prédicat :

```sql
-- Mode incrémentiel : filtre sur updated_at depuis le dernier MAX connu
WHERE updated_at >= (
  SELECT COALESCE(MAX(updated_at), '1900-01-01') - INTERVAL 3 DAYS
  FROM <this>
)

-- Mode full refresh : pas de filtre (1=1)
WHERE 1=1
```

### Fenêtre de lookback

La variable `incremental_lookback_days` (défaut : `3`) permet de relire les N derniers jours même s'ils ont déjà été chargés. Cela absorbe les mises à jour tardives et les corrections à la source.

```yaml
# dbt_project.yml
vars:
  incremental_lookback_days: 3
```

Pour forcer un recalcul complet d'un modèle :

```bash
dbt run --select brz_commandes --full-refresh
```

---

## Structure du projet

```
DBT-sandbox/
├── dbt_project.yml                  # Configuration centrale du projet
├── profiles.yml                     # Connexions Hive (gitignored)
├── packages.yml                     # Dépendances (dbt_utils)
├── requirements.txt                 # dbt-core + dbt-hive
│
├── macros/
│   ├── incremental_utils.sql        # Macros incrémentielles et partitions
│   ├── hive_utils.sql               # Utilitaires Hive (dédup, normalisation)
│   └── schema_utils.sql             # Génération des noms de schémas
│
├── models/
│   ├── bronze/
│   │   ├── _sources.yml             # Déclaration des tables sources raw
│   │   ├── _schema.yml              # Tests Bronze
│   │   ├── brz_commandes.sql
│   │   └── brz_clients.sql
│   ├── silver/
│   │   ├── _schema.yml              # Tests Silver
│   │   ├── slv_commandes.sql
│   │   └── slv_clients.sql
│   └── gold/
│       ├── _schema.yml              # Tests Gold
│       ├── gld_commandes_par_jour.sql
│       └── gld_kpi_clients.sql
│
├── seeds/
│   └── ref_statuts_commande.csv     # Référentiel statuts (EN_COURS, VALIDE…)
│
├── analyses/
│   └── exploration_incremental.sql  # Requêtes de vérification (dbt compile)
│
└── tests/
    └── generic/
        └── test_partition_non_vide.sql
```

---

## Macros Jinja réutilisables

### `incremental_utils.sql`

| Macro | Description |
|---|---|
| `get_max_timestamp(column)` | Retourne le MAX timestamp de la table cible, avec lookback |
| `incremental_partition_predicate(ts_column)` | Génère le WHERE pour filtrer les partitions impactées |
| `date_partition_cols(ts_column)` | Génère les colonnes `annee`, `mois`, `jour` depuis un timestamp |
| `month_partition_cols(ts_column)` | Génère les colonnes `annee`, `mois` (Gold) |

### `hive_utils.sql`

| Macro | Description |
|---|---|
| `deduplicate(relation, unique_key, order_col)` | ROW_NUMBER sur clé + tri par timestamp DESC |
| `normalize_string(col)` | `UPPER(TRIM(col))` |
| `hive_tblproperties()` | TBLPROPERTIES avec métadonnées dbt |

### `schema_utils.sql`

| Macro | Description |
|---|---|
| `generate_schema_name(custom_schema, node)` | Override dbt : génère `<target_schema>_<couche>` |

**Exemple d'usage dans un modèle Silver :**

```sql
{{ config(
    materialized        = 'incremental',
    incremental_strategy= 'insert_overwrite',
    partition_by        = ['annee', 'mois', 'jour']
) }}

WITH source AS (
  SELECT * FROM {{ ref('brz_commandes') }}
  WHERE {{ incremental_partition_predicate('updated_at') }}
),
dedup AS (
  {{ deduplicate('source', ['id_commande'], 'updated_at') }}
)
SELECT *, {{ date_partition_cols('updated_at') }}
FROM dedup
```

---

## Prérequis

- Python >= 3.9
- Accès à un cluster Hive avec HiveServer2 (Thrift ou HTTP)
- Variables d'environnement : `HIVE_HOST`, `HIVE_PORT`, `HIVE_USER`

---

## Installation

```bash
# 1. Cloner le repo
git clone <url-du-repo>
cd DBT-sandbox

# 2. Créer un environnement virtuel
python -m venv .venv
source .venv/bin/activate       # Windows : .venv\Scripts\activate

# 3. Installer les dépendances Python
pip install -r requirements.txt

# 4. Copier et renseigner le profil de connexion
cp profiles.yml ~/.dbt/profiles.yml
# éditer ~/.dbt/profiles.yml avec vos paramètres Hive

# 5. Installer les packages dbt
dbt deps

# 6. Vérifier la connexion
dbt debug
```

---

## Configuration

### Variables d'environnement

```bash
export HIVE_HOST=hiveserver2.moncluster.local
export HIVE_PORT=10000
export HIVE_USER=mon_user
# export HIVE_PASSWORD=...   # si authentification par mot de passe
```

### Cibles disponibles (`profiles.yml`)

| Cible | Schéma Hive | Threads | Usage |
|---|---|---|---|
| `dev` | `dev_sandbox_*` | 4 | Développement local |
| `prod` | `prod_sandbox_*` | 8 | Production |

```bash
# utiliser la cible dev (défaut)
dbt run

# utiliser la cible prod
dbt run --target prod
```

### Adapter les sources

Modifier `models/bronze/_sources.yml` pour pointer vers les vraies tables Hive :

```yaml
sources:
  - name: raw
    database: ma_base_raw     # ← nom réel de la base Hive source
    schema: ma_base_raw
    tables:
      - name: commandes       # ← nom réel de la table
```

---

## Usage

### Commandes essentielles

```bash
# Charger les référentiels (seeds)
dbt seed

# Lancer tous les modèles en mode incrémentiel
dbt run

# Lancer uniquement une couche
dbt run --select tag:bronze
dbt run --select tag:silver
dbt run --select tag:gold

# Lancer un modèle spécifique et ses dépendances amont
dbt run --select +slv_commandes

# Forcer le recalcul complet d'un modèle
dbt run --select brz_commandes --full-refresh

# Forcer le recalcul complet de toute la chaîne
dbt run --full-refresh

# Compiler les analyses sans les exécuter (utile pour Beeline)
dbt compile --select exploration_incremental
# → SQL disponible dans target/compiled/dbt_sandbox/analyses/
```

### Workflow de run journalier typique

```bash
# 1. Seeds (si le référentiel a changé)
dbt seed --select ref_statuts_commande

# 2. Bronze → Silver → Gold en séquence
dbt run --select tag:bronze
dbt run --select tag:silver
dbt run --select tag:gold

# 3. Tests
dbt test

# Ou tout en une commande
dbt build
```

### Ajuster la fenêtre de lookback à la volée

```bash
# Relire les 7 derniers jours au lieu de 3
dbt run --vars '{"incremental_lookback_days": 7}'
```

---

## Tests

```bash
# Lancer tous les tests
dbt test

# Tests d'une couche uniquement
dbt test --select tag:silver

# Tests + run en une commande
dbt build
```

### Tests inclus

| Couche | Modèle | Tests |
|---|---|---|
| Bronze | `brz_commandes` | `not_null` sur `id_commande`, `updated_at`, colonnes de partition |
| Bronze | `brz_clients` | `not_null` sur `id_client`, `updated_at` |
| Silver | `slv_commandes` | `not_null` + `unique` sur `id_commande` |
| Silver | `slv_clients` | `not_null` + `unique` sur `id_client` |
| Gold | `gld_commandes_par_jour` | `not_null` + `unique` sur `date_commande` |
| Gold | `gld_kpi_clients` | `not_null` sur `id_client`, `date_reference` |

Le test générique `test_partition_non_vide` peut être ajouté dans n'importe quel `_schema.yml` :

```yaml
models:
  - name: brz_commandes
    tests:
      - test_partition_non_vide:
          annee: 2024
          mois: 3
```
