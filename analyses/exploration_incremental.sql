/*
  Analyse exploratoire – Vérification des chargements incrémentiels
  -----------------------------------------------------------------
  Ce fichier n'est pas matérialisé dans Hive : il sert à dbt compile
  pour générer le SQL exploratoire que vous pouvez exécuter manuellement
  via Beeline / HiveServer2.

  Utilisation : dbt compile --select exploration_incremental
  Le SQL compilé se trouve dans target/compiled/...
*/

-- 1. Quelles partitions ont été chargées aujourd'hui ?
SELECT
  annee,
  mois,
  jour,
  COUNT(*)         AS nb_lignes,
  MAX(_loaded_at)  AS dernier_chargement
FROM {{ ref('brz_commandes') }}
WHERE _loaded_at >= DATE_SUB(CURRENT_DATE(), 1)
GROUP BY annee, mois, jour
ORDER BY annee, mois, jour;


-- 2. Vérification de la cohérence Silver vs Bronze sur les partitions récentes
SELECT
  brz.annee,
  brz.mois,
  brz.jour,
  COUNT(DISTINCT brz.id_commande)                            AS nb_brz,
  COUNT(DISTINCT slv.id_commande)                            AS nb_slv,
  COUNT(DISTINCT brz.id_commande) - COUNT(DISTINCT slv.id_commande)
                                                             AS delta_rejet
FROM {{ ref('brz_commandes') }}  brz
LEFT JOIN {{ ref('slv_commandes') }} slv
  ON brz.id_commande = slv.id_commande
WHERE brz.annee  = YEAR(CURRENT_DATE())
  AND brz.mois   = MONTH(CURRENT_DATE())
GROUP BY brz.annee, brz.mois, brz.jour
ORDER BY brz.jour;


-- 3. Évolution du CA journalier sur les 30 derniers jours
SELECT
  date_commande,
  nb_commandes,
  ca_ht_total,
  panier_moyen_ht,
  taux_annulation_pct
FROM {{ ref('gld_commandes_par_jour') }}
WHERE date_commande >= DATE_SUB(CURRENT_DATE(), 30)
ORDER BY date_commande;
