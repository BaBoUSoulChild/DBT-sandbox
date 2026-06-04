{{
  config(
    materialized        = 'incremental',
    incremental_strategy= 'insert_overwrite',
    file_format         = 'parquet',
    partition_by        = ['annee', 'mois', 'jour'],
    tags                = ['bronze', 'commandes']
  )
}}

/*
  Bronze – Commandes
  ------------------
  Ingestion minimale depuis la source brute.
  Aucune transformation métier : on copie fidèlement les données source
  en ajoutant uniquement les colonnes de partition et les métadonnées
  techniques de chargement.
*/

WITH source AS (

  SELECT
    id_commande,
    id_client,
    CAST(date_commande AS DATE)            AS date_commande,   -- date métier immuable → clé de partition Silver
    montant_ht,
    statut,
    updated_at,
    -- métadonnées techniques
    CURRENT_TIMESTAMP()                    AS _loaded_at,
    '{{ invocation_id }}'                  AS _dbt_invocation_id
  FROM {{ source('raw', 'commandes') }}
  WHERE {{ incremental_partition_predicate('updated_at') }}

)

SELECT
  id_commande,
  id_client,
  date_commande,
  montant_ht,
  statut,
  updated_at,
  _loaded_at,
  _dbt_invocation_id,
  -- Bronze partitionné par updated_at : raw zone, multi-versions acceptées
  {{ date_partition_cols('updated_at') }}
FROM source
