{#
  Génère la clause TBLPROPERTIES standard pour les tables Hive managées.
  Ajoute les métadonnées dbt (run_id, invocation_id) dans les propriétés.
#}
{% macro hive_tblproperties() %}
  TBLPROPERTIES (
    'dbt_project'       = '{{ project_name }}',
    'dbt_node'          = '{{ model.unique_id }}',
    'dbt_invocation_id' = '{{ invocation_id }}'
  )
{% endmacro %}


{#
  Macro de déduplication : conserve la dernière version d'une ligne
  selon une clé et un timestamp, ce qui est utile en Silver/Gold.

  Paramètres:
    relation   : nom de la CTE ou table à dédupliquer
    unique_key : liste des colonnes formant la clé
    order_col  : colonne de tri (timestamp de mise à jour)
#}
{% macro deduplicate(relation, unique_key, order_col='updated_at') %}
  SELECT *
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY {{ unique_key | join(', ') }}
        ORDER BY {{ order_col }} DESC
      ) AS _rn
    FROM {{ relation }}
  ) ranked
  WHERE _rn = 1
{% endmacro %}


{#
  Normalise une chaîne : trim + uppercase.
  Pratique pour les colonnes de référentiel en Silver.
#}
{% macro normalize_string(col) %}
  UPPER(TRIM({{ col }}))
{% endmacro %}
