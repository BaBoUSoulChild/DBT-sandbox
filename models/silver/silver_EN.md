{# English translations of Silver doc blocks.
   To activate: replace doc('slv_commandes') references in _schema.yml
   with doc('slv_commandes_en'), etc.
#}

{% docs slv_commandes_en %}
Cleaned and enriched orders from the Bronze layer. Partitioned by `updated_at`
— same axis as Bronze.

**Multi-version behaviour (by design):** a modified order generates a new version
in the `updated_at` partition of the modification date. Previous versions remain
in their original partitions. `id_commande` is therefore unique *within a partition*
but not at the global table level. Inter-partition deduplication is delegated to
Gold models via `deduplicate()`.

This choice is justified by the nature of the data: updates affect orders up to
several years old, scattered across many partitions. Rewriting partitions by
business date would be prohibitively expensive.

Transformations applied:
- **Intra-partition deduplication**: one version per `id_commande` within the
  current run's `updated_at` window, via `ROW_NUMBER()`.
- **Enrichment**: join with `ref_statuts_commande` for the normalised status label.
- **Rejection**: rows without `id_client` or `montant_ht` are excluded.
- **Cast**: `montant_ht` cast to `DECIMAL(18,2)`.
{% enddocs %}

{% docs slv_clients_en %}
Cleaned and normalised client reference table from the Bronze layer.

Transformations applied:
- **Deduplication**: latest version per `id_client`.
- **Normalisation**: `nom` in uppercase without trailing spaces, `email` in lowercase,
  `pays` in uppercase.
- **GDPR masking**: the `email_masque` column replaces the local part of the email
  with its MD5 hash (`<md5>@domain.com`) for analytical use cases that do not
  require the plain-text email.
- **Rejection**: rows without `id_client` are excluded.
{% enddocs %}

{% docs slv_col_statut_code_en %}
Normalised status code (UPPER + TRIM). Expected values defined in the
`ref_statuts_commande` seed: `EN_COURS`, `VALIDE`, `EXPEDIE`, `LIVRE`,
`ANNULE`, `REMBOURSE`.
{% enddocs %}

{% docs slv_col_statut_libelle_en %}
Human-readable status label from the join with `ref_statuts_commande`.
Falls back to the normalised raw code if the source code is unknown to the
reference table.
{% enddocs %}

{% docs slv_col_email_masque_en %}
Email with local part replaced by its MD5 hash. Format: `<md5>@domain.com`.
To be used for any analytical purpose that does not require nominal client
identification (GDPR compliance).
{% enddocs %}

{% docs slv_col_montant_ht_en %}
Order amount excluding tax in euros, cast to `DECIMAL(18,2)`.
Always positive and non-null at this layer (null rows rejected in Bronze→Silver).
{% enddocs %}
