{{
  config(
    materialized        = 'incremental',
    incremental_strategy= 'insert_overwrite',
    file_format         = 'parquet',
    partition_by        = ['annee', 'mois', 'jour'],
    tags                = ['silver', 'commandes']
  )
}}

/*
  Silver – Commandes
  ------------------
  - Dédoublonnage sur id_commande (on garde la version la plus récente)
  - Normalisation du statut via le référentiel seed
  - Rejet des lignes sans montant ou sans client
  - Cast des types
*/

WITH brz AS (

  SELECT *
  FROM {{ ref('brz_commandes') }}
  WHERE {{ incremental_partition_predicate('updated_at') }}

),

-- jointure avec le référentiel statuts pour libellés normalisés
ref_statuts AS (
  SELECT code_statut, libelle_statut
  FROM {{ ref('ref_statuts_commande') }}
),

enriched AS (

  SELECT
    b.id_commande,
    b.id_client,
    CAST(b.montant_ht AS DECIMAL(18, 2))          AS montant_ht,
    {{ normalize_string('b.statut') }}             AS statut_code,
    COALESCE(r.libelle_statut, b.statut)           AS statut_libelle,
    b.updated_at,
    b._loaded_at,
    b._dbt_invocation_id
  FROM brz b
  LEFT JOIN ref_statuts r
    ON {{ normalize_string('b.statut') }} = r.code_statut
  WHERE b.id_client  IS NOT NULL
    AND b.montant_ht IS NOT NULL

),

-- dédoublonnage : on garde la ligne la plus récente par commande
dedup AS (
  {{ deduplicate('enriched', ['id_commande'], 'updated_at') }}
)

SELECT
  id_commande,
  id_client,
  montant_ht,
  statut_code,
  statut_libelle,
  updated_at,
  _loaded_at,
  _dbt_invocation_id,
  {{ date_partition_cols('updated_at') }}
FROM dedup
