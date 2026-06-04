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
  Partitionnement par DATE_COMMANDE (date métier stable, immuable)
  et non par updated_at — c'est la clé de la gestion correcte du
  INSERT OVERWRITE sur Hive non-ACID.

  Pourquoi ce choix est critique :
    INSERT OVERWRITE dynamique Hive n'écrase QUE les partitions présentes
    dans le résultat. Si on partitionne par updated_at, une commande modifiée
    migre vers une nouvelle partition et laisse l'ancienne partition Silver
    avec l'ancienne version → duplicats inter-partitions.
    En partitionnant par date_commande (immuable), la commande reste toujours
    dans la même partition, que ce soit à la création ou à la 10e modification.

  Stratégie incrémentielle (macro load_affected_business_partitions) :
    1. Détecter les date_commande des lignes modifiées (filtre sur updated_at)
    2. Recharger TOUTES les lignes Bronze de ces partitions métier (pas seulement
       les lignes modifiées) pour garantir une déduplication complète
    3. Le INSERT OVERWRITE remplace la partition date_commande entière → propre
*/

WITH brz AS (

  -- Recharge les partitions métier complètes affectées par les changements récents
  {{ load_affected_business_partitions(
      ref('brz_commandes'),
      ts_column        = 'updated_at',
      business_date_col= 'date_commande'
  ) }}

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

-- Dédoublonnage sur la partition complète rechargée → une seule version par commande
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
  -- Partition par date_commande (stable) — jamais par updated_at en Silver
  {{ date_partition_cols('date_commande') }}
FROM dedup
