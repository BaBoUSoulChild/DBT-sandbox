{#
  test_taux_rejet_max
  -------------------
  Vérifie que le taux de perte de clés fonctionnelles entre la couche source
  (Bronze) et la couche cible (Silver) ne dépasse pas un seuil configurable.

  Le test ÉCHOUE (retourne une ligne) si :
      (nb_source - nb_cible) / nb_source * 100  >  seuil_pct

  Une perte nulle est normale (déduplication, rejets de lignes invalides).
  Ce test détecte une perte ANORMALE par rapport au seuil attendu.

  Usage dans _schema.yml (niveau modèle) :
    tests:
      - test_taux_rejet_max:
          source_model: ref('brz_commandes')
          key_column: id_commande
          seuil_pct: 5        # 5 % de perte max autorisée
#}
{% test taux_rejet_max(model, source_model, key_column, seuil_pct=5) %}

SELECT
  nb_source,
  nb_cible,
  ROUND(
    CAST(nb_source - nb_cible AS DOUBLE) / nb_source * 100.0, 2
  ) AS taux_rejet_pct,
  {{ seuil_pct }} AS seuil_pct
FROM (
  SELECT
    (SELECT COUNT(DISTINCT {{ key_column }}) FROM {{ source_model }}) AS nb_source,
    (SELECT COUNT(DISTINCT {{ key_column }}) FROM {{ model }})        AS nb_cible
) comptage
WHERE nb_source > 0
  AND (CAST(nb_source - nb_cible AS DOUBLE) / nb_source) * 100.0 > {{ seuil_pct }}

{% endtest %}
