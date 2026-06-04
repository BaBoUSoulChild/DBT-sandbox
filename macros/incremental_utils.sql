{#
  Retourne le timestamp MAX de la table cible.
  Utilisé comme borne basse pour les chargements incrémentiels.
  Inclut un lookback configurable pour absorber les arrivées tardives.
#}
{% macro get_max_timestamp(column='updated_at') %}
  {%- if is_incremental() -%}
    (
      SELECT
        COALESCE(
          MAX({{ column }}),
          CAST('1900-01-01' AS TIMESTAMP)
        ) - INTERVAL {{ var('incremental_lookback_days') }} DAYS
      FROM {{ this }}
    )
  {%- else -%}
    CAST('1900-01-01' AS TIMESTAMP)
  {%- endif -%}
{% endmacro %}


{#
  Génère le prédicat WHERE pour filtrer uniquement les partitions
  année/mois/jour affectées par la mise à jour incrémentielle.
  Cela évite un full-scan de la table source.
#}
{% macro incremental_partition_predicate(ts_column='updated_at', lookback_days=none) %}
  {%- set lb = lookback_days if lookback_days is not none else var('incremental_lookback_days') -%}
  {%- if is_incremental() -%}
    {{ ts_column }} >= {{ get_max_timestamp(ts_column) }}
  {%- else -%}
    1=1
  {%- endif -%}
{% endmacro %}


{#
  Extrait les colonnes de partition date depuis un timestamp.
  À inclure dans chaque SELECT pour alimenter les partitions Hive.
#}
{% macro date_partition_cols(ts_column='updated_at') %}
  YEAR({{ ts_column }})  AS annee,
  MONTH({{ ts_column }}) AS mois,
  DAY({{ ts_column }})   AS jour
{% endmacro %}


{#
  Variante gold : partition annee/mois uniquement (granularité mensuelle).
#}
{% macro month_partition_cols(ts_column='updated_at') %}
  YEAR({{ ts_column }})  AS annee,
  MONTH({{ ts_column }}) AS mois
{% endmacro %}


{#
  load_affected_business_partitions  [Silver non-ACID Hive]
  ----------------------------------------------------------
  Problème fondamental du INSERT OVERWRITE sur Hive non-ACID :
    - Le INSERT OVERWRITE dynamique n'écrase QUE les partitions présentes dans
      le résultat. Les autres partitions sont intactes.
    - Si on partitionne Silver par updated_at, une commande modifiée (dont
      updated_at change) migre vers une nouvelle partition, laissant l'ancienne
      partition Silver avec l'ancienne version → duplicats inter-partitions.

  Solution : partitionner Silver par une DATE MÉTIER STABLE (ex: date_commande),
  immuable pour la durée de vie de l'enregistrement.
  Cette macro :
    1. Identifie les partitions métier (date_commande) contenant des lignes
       modifiées depuis le dernier run (via updated_at).
    2. Recharge TOUTES les lignes Bronze appartenant à ces partitions métier
       (pas seulement celles modifiées).
    3. Le INSERT OVERWRITE écrase ensuite la partition Silver complète
       → déduplication propre, zéro doublon inter-partitions.

  Paramètres:
    source_model      : modèle Bronze (ref())
    ts_column         : colonne de détection des changements  (défaut: updated_at)
    business_date_col : date métier stable, immuable          (défaut: date_commande)

  Prérequis : business_date_col doit être présente dans la source Bronze
  et NE PAS changer au cours de la vie de l'enregistrement.
#}
{% macro load_affected_business_partitions(source_model, ts_column='updated_at', business_date_col='date_commande') %}

  {%- if is_incremental() -%}

    WITH _partitions_impactees AS (
      -- Étape 1 : identifier les dates métier des lignes modifiées récemment
      -- Seule cette sous-requête utilise updated_at — le reste filtre sur la date stable
      SELECT DISTINCT CAST({{ business_date_col }} AS DATE) AS _date_ref
      FROM {{ source_model }}
      WHERE {{ ts_column }} >= {{ get_max_timestamp(ts_column) }}
    )
    -- Étape 2 : recharger TOUTES les lignes de ces partitions métier (toutes versions)
    -- Le ROW_NUMBER de déduplication Silver sélectionnera ensuite la plus récente
    SELECT src.*
    FROM {{ source_model }} src
    INNER JOIN _partitions_impactees pi
      ON CAST(src.{{ business_date_col }} AS DATE) = pi._date_ref

  {%- else -%}

    SELECT * FROM {{ source_model }}

  {%- endif -%}

{% endmacro %}
