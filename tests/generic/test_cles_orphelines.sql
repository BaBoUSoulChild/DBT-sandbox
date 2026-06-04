{#
  test_cles_orphelines
  --------------------
  Vérifie qu'aucune clé de la couche cible (Silver) n'est absente de la
  couche source (Bronze). Retourne les clés fantômes introduites par un
  bug de jointure ou une transformation incorrecte.

  Le test ÉCHOUE (retourne des lignes) si une valeur de la colonne testée
  dans `model` ne se trouve pas dans `source_model`.

  Test de niveau COLONNE : dbt passe automatiquement `column_name`.

  Usage dans _schema.yml :
    columns:
      - name: id_commande
        tests:
          - test_cles_orphelines:
              source_model: ref('brz_commandes')
#}
{% test cles_orphelines(model, column_name, source_model) %}

SELECT cible.{{ column_name }}
FROM {{ model }}        AS cible
LEFT JOIN {{ source_model }} AS src
  ON cible.{{ column_name }} = src.{{ column_name }}
WHERE src.{{ column_name }} IS NULL

{% endtest %}
