{% docs slv_commandes %}
Commandes nettoyées et enrichies, issues de la couche Bronze. Partitionnée par
`updated_at` (même axe que Bronze).

**Comportement multi-versions (par design) :** une commande modifiée génère une
nouvelle version dans la partition `updated_at` du jour de modification. Les
versions précédentes restent dans leurs partitions d'origine. `id_commande` est
donc unique *au sein d'une partition* mais pas au niveau de la table globale.
La déduplication inter-partitions est déléguée aux modèles Gold via `deduplicate()`.

Ce choix est justifié par la nature des données : les mises à jour touchent des
commandes vieilles de plusieurs années, éparpillées sur de nombreuses partitions.
Réécrire les partitions par date métier serait prohibitivement coûteux.

Transformations appliquées :
- **Dédoublonnage intra-partition** : une seule version par `id_commande` dans
  la fenêtre `updated_at` du run courant via `ROW_NUMBER()`.
- **Enrichissement** : jointure avec `ref_statuts_commande` pour le libellé statut.
- **Rejet** : lignes sans `id_client` ou sans `montant_ht` exclues.
- **Cast** : `montant_ht` casté en `DECIMAL(18,2)`.
{% enddocs %}

{% docs slv_clients %}
Référentiel clients nettoyé et normalisé, issu de la couche Bronze.

Transformations appliquées :
- **Dédoublonnage** : dernière version par `id_client`.
- **Normalisation** : `nom` en majuscules sans espaces, `email` en minuscules, `pays`
  en majuscules.
- **Masquage RGPD** : la colonne `email_masque` remplace la partie locale de l'email
  par son hash MD5 (`<md5>@domaine.com`) pour les usages analytiques ne nécessitant
  pas l'email en clair.
- **Rejet** : les lignes sans `id_client` sont exclues.
{% enddocs %}

{% docs slv_col_statut_code %}
Code statut normalisé (UPPER + TRIM). Valeurs attendues définies dans le seed
`ref_statuts_commande` : `EN_COURS`, `VALIDE`, `EXPEDIE`, `LIVRE`, `ANNULE`, `REMBOURSE`.
{% enddocs %}

{% docs slv_col_statut_libelle %}
Libellé lisible du statut, issu de la jointure avec `ref_statuts_commande`.
Vaut le code brut normalisé si le code source est inconnu du référentiel.
{% enddocs %}

{% docs slv_col_email_masque %}
Email avec partie locale remplacée par son hash MD5. Format : `<md5>@domaine.com`.
À utiliser pour tout usage analytique ne nécessitant pas l'identification nominative
du client (conformité RGPD).
{% enddocs %}

{% docs slv_col_montant_ht %}
Montant hors taxe de la commande en euros, casté en `DECIMAL(18,2)`.
Toujours positif et non nul à cette couche (lignes nulles rejetées en Bronze→Silver).
{% enddocs %}
