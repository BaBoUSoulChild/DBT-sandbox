{#
  test_couverture_agregat
  -----------------------
  Vérifie que chaque valeur de la colonne d'agrégation dans la couche Gold
  correspond à au moins une ligne dans la couche Silver.

  Détecte les trous dans les agrégats : une date présente en Gold mais sans
  aucune donnée correspondante en Silver signale une incohérence de pipeline.

  Test de niveau COLONNE : dbt passe automatiquement `column_name`
  (la colonne d'agrégation Gold, ex : date_commande).

  Usage dans _schema.yml :
    columns:
      - name: date_commande
        tests:
          - test_couverture_agregat:
              source_model: ref('slv_commandes')
              date_column_source: updated_at
#}
{% test couverture_agregat(model, column_name, source_model, date_column_source) %}

WITH dates_silver AS (
  SELECT DISTINCT CAST({{ date_column_source }} AS DATE) AS date_ref
  FROM {{ source_model }}
)

SELECT gold.{{ column_name }}
FROM {{ model }} AS gold
LEFT JOIN dates_silver AS slv
  ON gold.{{ column_name }} = slv.date_ref
WHERE slv.date_ref IS NULL

{% endtest %}
