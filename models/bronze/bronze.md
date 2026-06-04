{% docs brz_commandes %}
Copie brute et non transformée du flux de commandes issu du système transactionnel source.

Chaque run incrémentiel réécrit uniquement les partitions `annee/mois/jour` dont le
champ `updated_at` a évolué depuis le dernier chargement (fenêtre de lookback : 3 jours).
Aucune logique métier n'est appliquée à cette couche : les doublons, les valeurs nulles
et les codes statut non normalisés sont conservés tels quels pour garantir la traçabilité
complète des données source.
{% enddocs %}

{% docs brz_clients %}
Copie brute du référentiel clients extrait du système source.

Partitionné par date de dernière modification (`updated_at`). Cette table contient
l'historique brut complet des versions successives de chaque client, y compris les
doublons et les emails non normalisés. La déduplication et la normalisation sont
déléguées à la couche Silver.
{% enddocs %}

{% docs brz_col_id_commande %}
Identifiant technique unique de la commande tel que fourni par le système source.
Peut apparaître plusieurs fois en Bronze si la commande a été modifiée (plusieurs
versions d'une même clé sur des partitions différentes).
{% enddocs %}

{% docs brz_col_id_client %}
Clé étrangère vers le client passeur de la commande. Non validée à cette couche :
peut être nulle ou pointer vers un client inexistant dans le référentiel.
{% enddocs %}

{% docs brz_col_statut %}
Code statut brut issu du système source. Non normalisé : casse et espaces variables
possibles (ex. `"valide"`, `"VALIDE "`, `"Valide"`). La normalisation est faite en Silver.
{% enddocs %}

{% docs brz_col_updated_at %}
Timestamp de dernière modification de l'enregistrement dans le système source.
Colonne pivot du partitionnement Hive et du filtre incrémentiel.
{% enddocs %}

{% docs brz_col_loaded_at %}
Timestamp du moment où la ligne a été chargée par dbt dans la couche Bronze.
Permet de distinguer la date de modification source (`updated_at`) de la date
d'arrivée dans le data lake.
{% enddocs %}

{% docs brz_col_partition_annee %}
Année extraite de `updated_at`. Colonne de partitionnement Hive — toujours en
dernière position dans le SELECT pour respecter la convention Hive.
{% enddocs %}

{% docs brz_col_partition_mois %}
Mois extrait de `updated_at` (1–12). Colonne de partitionnement Hive.
{% enddocs %}

{% docs brz_col_partition_jour %}
Jour extrait de `updated_at` (1–31). Colonne de partitionnement Hive.
{% enddocs %}
