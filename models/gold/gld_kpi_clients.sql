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
  Calcul des indicateurs clients sur le mois courant + lookback.
  Recalcule les mois impactés par les mises à jour Silver.
*/

WITH commandes AS (

  SELECT
    id_client,
    montant_ht,
    statut_code,
    CAST(updated_at AS DATE) AS date_commande,
    annee,
    mois
  FROM {{ ref('slv_commandes') }}

  {% if is_incremental() %}
  WHERE DATE_TRUNC('MONTH', CAST(updated_at AS DATE))
        >= (
          SELECT DATE_TRUNC('MONTH',
            COALESCE(MAX(date_reference), CAST('1900-01-01' AS DATE))
            - INTERVAL {{ var('incremental_lookback_days') }} DAYS
          )
          FROM {{ this }}
        )
  {% endif %}

),

clients AS (
  SELECT id_client, nom, pays
  FROM {{ ref('slv_clients') }}
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
  INNER JOIN clients cl ON c.id_client = cl.id_client
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
