{{
  config(
    materialized        = 'incremental',
    incremental_strategy= 'insert_overwrite',
    file_format         = 'parquet',
    partition_by        = ['annee', 'mois'],
    tags                = ['gold', 'clients']
  )
}}

/*
  Gold – KPI clients (snapshot mensuel glissant)
  ------------------------------------------------
  Silver étant multi-versions par updated_at, déduplication obligatoire
  avant tout agrégat (même raison que gld_commandes_par_jour).
*/

{% set max_ts_query %}
  SELECT DATE_TRUNC('MONTH',
    COALESCE(MAX(date_reference), CAST('1900-01-01' AS DATE))
    - INTERVAL {{ var('incremental_lookback_days') }} DAYS
  )
  FROM {{ this }}
{% endset %}

WITH slv_commandes_dedup AS (

  -- Déduplication inter-partitions Silver : version la plus récente par commande
  {{ deduplicate(ref('slv_commandes'), ['id_commande'], 'updated_at') }}

),

slv_clients_dedup AS (

  -- Même principe pour les clients (référentiel potentiellement multi-versions)
  {{ deduplicate(ref('slv_clients'), ['id_client'], 'updated_at') }}

),

commandes AS (

  SELECT
    id_client,
    montant_ht,
    statut_code,
    date_commande
  FROM slv_commandes_dedup

  {% if is_incremental() %}
  WHERE DATE_TRUNC('MONTH', date_commande) >= ({{ max_ts_query }})
  {% endif %}

),

kpi AS (

  SELECT
    c.id_client,
    cl.nom                                                               AS nom_client,
    cl.pays,
    DATE_TRUNC('MONTH', c.date_commande)                                 AS date_reference,
    COUNT(*)                                                             AS nb_commandes_mois,
    SUM(c.montant_ht)                                                    AS ca_ht_mois,
    AVG(c.montant_ht)                                                    AS panier_moyen_mois,
    MIN(c.date_commande)                                                 AS premiere_commande_mois,
    MAX(c.date_commande)                                                 AS derniere_commande_mois,
    SUM(CASE WHEN c.statut_code = 'ANNULE' THEN 1 ELSE 0 END)           AS nb_annulations_mois,
    CURRENT_TIMESTAMP()                                                  AS _loaded_at
  FROM commandes c
  INNER JOIN slv_clients_dedup cl ON c.id_client = cl.id_client
  GROUP BY
    c.id_client, cl.nom, cl.pays,
    DATE_TRUNC('MONTH', c.date_commande)

)

SELECT
  id_client,
  nom_client,
  pays,
  date_reference,
  nb_commandes_mois,
  ROUND(ca_ht_mois, 2)                           AS ca_ht_mois,
  ROUND(panier_moyen_mois, 2)                    AS panier_moyen_mois,
  premiere_commande_mois,
  derniere_commande_mois,
  nb_annulations_mois,
  _loaded_at,
  {{ month_partition_cols('date_reference') }}
FROM kpi
