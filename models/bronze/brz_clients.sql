{{
  config(
    materialized        = 'incremental',
    incremental_strategy= 'insert_overwrite',
    file_format         = 'parquet',
    partition_by        = ['annee', 'mois', 'jour'],
    tags                = ['bronze', 'clients']
  )
}}

/*
  Bronze – Clients
  ----------------
  Copie brute du référentiel clients.
*/

WITH source AS (

  SELECT
    id_client,
    nom,
    email,
    pays,
    updated_at,
    CURRENT_TIMESTAMP()   AS _loaded_at,
    '{{ invocation_id }}' AS _dbt_invocation_id
  FROM {{ source('raw', 'clients') }}
  WHERE {{ incremental_partition_predicate('updated_at') }}

)

SELECT
  id_client,
  nom,
  email,
  pays,
  updated_at,
  _loaded_at,
  _dbt_invocation_id,
  {{ date_partition_cols('updated_at') }}
FROM source
