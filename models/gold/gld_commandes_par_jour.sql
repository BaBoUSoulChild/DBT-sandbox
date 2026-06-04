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
  Agrégation journalière des commandes : CA, volume, panier moyen.
  Partition mensuelle (granularité Gold réduite).

  Utilise date_commande directement depuis Silver (date métier stable)
  plutôt qu'un CAST(updated_at AS DATE) — Silver garantit maintenant
  qu'une commande a toujours la même date_commande quelle que soit
  la partition dans laquelle elle a été modifiée.

  En mode incrémentiel, on recalcule tous les jours du mois impacté
  pour garantir la cohérence des agrégats même en cas d'arrivée tardive.
*/

{% set max_ts_query %}
  SELECT
    DATE_TRUNC('MONTH',
      COALESCE(MAX(date_commande), CAST('1900-01-01' AS DATE))
      - INTERVAL {{ var('incremental_lookback_days') }} DAYS
    )
  FROM {{ this }}
{% endset %}

WITH slv AS (

  SELECT
    date_commande,
    id_client,
    montant_ht,
    statut_code,
    annee,
    mois
  FROM {{ ref('slv_commandes') }}

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
