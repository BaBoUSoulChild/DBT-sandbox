{% docs gld_commandes_par_jour %}
Agrégats journaliers des commandes, calculés à partir de la couche Silver.

Indicateurs disponibles : chiffre d'affaires HT total et validé, nombre de commandes,
nombre de clients distincts, panier moyen, nombre et taux d'annulations.

En mode incrémentiel, le modèle recalcule tous les jours appartenant au mois courant
(et aux mois couverts par la fenêtre de lookback) pour garantir la cohérence des
agrégats même en cas d'arrivée tardive de données Silver. La partition Gold est au
niveau `annee/mois`, plus grossière que Bronze/Silver.

Ce modèle alimente le dashboard commercial quotidien.
{% enddocs %}

{% docs gld_kpi_clients %}
KPI clients calculés mois par mois à partir des commandes Silver et du référentiel
clients Silver.

Pour chaque couple `(id_client, mois)`, on dispose du CA HT, du panier moyen, du
nombre de commandes, du nombre d'annulations, et des dates de première et dernière
commande du mois. Ce modèle est la source principale du reporting fidélité et de la
segmentation client.

En mode incrémentiel, les mois impactés par des mises à jour Silver sont recalculés
entièrement pour garantir la cohérence des agrégats.
{% enddocs %}

{% docs gld_col_ca_ht_total %}
Chiffre d'affaires hors taxe total du jour, en euros, tous statuts confondus
(y compris les commandes annulées ou remboursées).
{% enddocs %}

{% docs gld_col_ca_ht_valide %}
Chiffre d'affaires hors taxe des commandes au statut `VALIDE` uniquement.
Indicateur métier principal pour le suivi des ventes confirmées.
{% enddocs %}

{% docs gld_col_panier_moyen_ht %}
Montant moyen HT par commande sur la journée, arrondi à 2 décimales.
{% enddocs %}

{% docs gld_col_taux_annulation_pct %}
Pourcentage de commandes annulées sur le total du jour, arrondi à 2 décimales.
Un pic sur cet indicateur peut signaler un problème opérationnel ou une campagne
promotionnelle défaillante.
{% enddocs %}

{% docs gld_col_date_reference %}
Premier jour du mois de référence pour le calcul des KPI clients (DATE_TRUNC MONTH).
Sert de clé de jointure temporelle pour les outils BI.
{% enddocs %}
