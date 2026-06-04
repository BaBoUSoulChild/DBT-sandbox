/*
  Test générique : vérifie qu'une partition donnée n'est pas vide.
  Échec si aucune ligne ne correspond aux paramètres fournis.

  Usage dans _schema.yml :
    tests:
      - test_partition_non_vide:
          annee: 2024
          mois: 1
*/
{% test test_partition_non_vide(model, annee, mois) %}
  SELECT 1
  FROM {{ model }}
  WHERE annee = {{ annee }}
    AND mois  = {{ mois }}
  HAVING COUNT(*) = 0
{% endtest %}
