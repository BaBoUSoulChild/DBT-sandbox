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
