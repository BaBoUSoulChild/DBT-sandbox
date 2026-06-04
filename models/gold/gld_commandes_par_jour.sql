{{
  config(
    materialized        = 'incremental',
    incremental_strategy= 'insert_overwrite',
    file_format         = 'parquet',
    partition_by        = ['annee', 'mois'],
    tags                = ['gold', 'commandes']
  )
}}

/*
  Gold – Commandes agrégées par jour
  ------------------------------------
  Silver étant multi-versions par updated_at, la première étape est
  obligatoire : déduplication inter-partitions avant tout agrégat.
  Sans ce CTE, une commande modifiée serait comptabilisée plusieurs fois
  (une fois par partition Silver dans laquelle elle apparaît).

  Utilise date_commande (date métier stable, issue de la source via Bronze)
  comme axe d'agrégation — pas updated_at.
*/

{% set max_ts_query %}
  SELECT
    DATE_TRUNC('MONTH',
      COALESCE(MAX(date_commande), CAST('1900-01-01' AS DATE))
      - INTERVAL {{ var('incremental_lookback_days') }} DAYS
    )
  FROM {{ this }}
{% endset %}

WITH slv_dedup AS (

  -- Étape obligatoire : Silver est multi-versions (partitionné par updated_at).
  -- On conserve uniquement la version la plus récente de chaque commande
  -- avant d'agréger, sinon les montants et comptages seraient faux.
  {{ deduplicate(ref('slv_commandes'), ['id_commande'], 'updated_at') }}

),

slv AS (

  SELECT
    date_commande,
    id_client,
    montant_ht,
    statut_code
  FROM slv_dedup

  {% if is_incremental() %}
  WHERE DATE_TRUNC('MONTH', date_commande) >= ({{ max_ts_query }})
  {% endif %}

),

aggregated AS (

  SELECT
    date_commande,
    COUNT(*)                                                           AS nb_commandes,
    COUNT(DISTINCT id_client)                                          AS nb_clients_distincts,
    SUM(montant_ht)                                                    AS ca_ht_total,
    AVG(montant_ht)                                                    AS panier_moyen_ht,
    SUM(CASE WHEN statut_code = 'VALIDE' THEN montant_ht ELSE 0 END)  AS ca_ht_valide,
    SUM(CASE WHEN statut_code = 'ANNULE' THEN 1        ELSE 0 END)    AS nb_annulations,
    MAX(CURRENT_TIMESTAMP())                                           AS _loaded_at
  FROM slv
  GROUP BY date_commande

)

SELECT
  date_commande,
  nb_commandes,
  nb_clients_distincts,
  ca_ht_total,
  ROUND(panier_moyen_ht, 2)                     AS panier_moyen_ht,
  ca_ht_valide,
  nb_annulations,
  ROUND(
    100.0 * nb_annulations / NULLIF(nb_commandes, 0), 2
  )                                             AS taux_annulation_pct,
  _loaded_at,
  {{ month_partition_cols('date_commande') }}
FROM aggregated
