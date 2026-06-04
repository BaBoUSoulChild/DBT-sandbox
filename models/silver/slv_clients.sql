{{
  config(
    materialized        = 'incremental',
    incremental_strategy= 'insert_overwrite',
    file_format         = 'parquet',
    partition_by        = ['annee', 'mois', 'jour'],
    tags                = ['silver', 'clients']
  )
}}

/*
  Silver – Clients
  ----------------
  - Dédoublonnage sur id_client
  - Normalisation email (lowercase) et pays (uppercase)
  - Masquage partiel de l'email (RGPD sandbox)
*/

WITH brz AS (

  SELECT *
  FROM {{ ref('brz_clients') }}
  WHERE {{ incremental_partition_predicate('updated_at') }}

),

enriched AS (

  SELECT
    id_client,
    {{ normalize_string('nom') }}                            AS nom,
    LOWER(TRIM(email))                                       AS email,
    -- masquage partiel : keep domaine, hash local part
    CONCAT(
      MD5(SPLIT(LOWER(TRIM(email)), '@')[0]),
      '@',
      SPLIT(LOWER(TRIM(email)), '@')[1]
    )                                                        AS email_masque,
    {{ normalize_string('pays') }}                           AS pays,
    updated_at,
    _loaded_at,
    _dbt_invocation_id
  FROM brz
  WHERE id_client IS NOT NULL

),

dedup AS (
  {{ deduplicate('enriched', ['id_client'], 'updated_at') }}
)

SELECT
  id_client,
  nom,
  email,
  email_masque,
  pays,
  updated_at,
  _loaded_at,
  _dbt_invocation_id,
  {{ date_partition_cols('updated_at') }}
FROM dedup
