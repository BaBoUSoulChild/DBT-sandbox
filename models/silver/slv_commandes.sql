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
  Partitionnement par UPDATED_AT — même convention que Bronze.

  Choix architectural : Silver est multi-versions par design.
  Une commande modifiée génère une nouvelle version dans la partition
  updated_at du jour de modification. Les versions précédentes restent
  dans leurs partitions d'origine.

  Pourquoi ne pas partitionner par date_commande :
  Les mises à jour touchent des commandes vieilles de plusieurs années,
  éparpillées sur de nombreuses partitions. Réécrire les partitions
  métier serait prohibitivement coûteux sans gain réel.

  Conséquence : id_commande n'est PAS unique au niveau de la table Silver
  globale — il l'est au niveau de chaque partition updated_at.
  La déduplication inter-partitions est déléguée aux modèles Gold.
*/

WITH brz AS (

  SELECT *
  FROM {{ ref('brz_commandes') }}
  WHERE {{ incremental_partition_predicate('updated_at') }}

),

ref_statuts AS (
  SELECT code_statut, libelle_statut
  FROM {{ ref('ref_statuts_commande') }}
),

enriched AS (

  SELECT
    b.id_commande,
    b.id_client,
    b.date_commande,
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

-- Déduplication INTRA-PARTITION uniquement : une version par id_commande
-- dans la fenêtre updated_at du run courant.
-- La déduplication INTER-PARTITIONS (toutes versions confondues) est faite en Gold.
dedup AS (
  {{ deduplicate('enriched', ['id_commande'], 'updated_at') }}
)

SELECT
  id_commande,
  id_client,
  date_commande,
  montant_ht,
  statut_code,
  statut_libelle,
  updated_at,
  _loaded_at,
  _dbt_invocation_id,
  {{ date_partition_cols('updated_at') }}
FROM dedup
