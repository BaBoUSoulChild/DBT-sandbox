{# English translations of Bronze doc blocks.
   To activate: replace doc('brz_commandes') references in _schema.yml
   with doc('brz_commandes_en'), etc.
#}

{% docs brz_commandes_en %}
Unmodified raw copy of the orders stream from the transactional source system.

Each incremental run rewrites only the `annee/mois/jour` partitions whose
`updated_at` field has changed since the last load (lookback window: 3 days).
No business logic is applied at this layer: duplicates, null values and
non-normalised status codes are preserved as-is to guarantee full traceability
of source data.
{% enddocs %}

{% docs brz_clients_en %}
Raw copy of the client reference table extracted from the source system.

Partitioned by last modification date (`updated_at`). This table contains
the complete raw history of successive versions of each client, including
duplicates and non-normalised emails. Deduplication and normalisation are
delegated to the Silver layer.
{% enddocs %}

{% docs brz_col_id_commande_en %}
Technical unique identifier of the order as provided by the source system.
May appear multiple times in Bronze if the order has been modified (multiple
versions of the same key across different partitions).
{% enddocs %}

{% docs brz_col_id_client_en %}
Foreign key to the client who placed the order. Not validated at this layer:
may be null or point to a client not present in the reference table.
{% enddocs %}

{% docs brz_col_statut_en %}
Raw status code from the source system. Not normalised: variable casing and
whitespace possible (e.g. `"valid"`, `"VALID "`, `"Valid"`). Normalisation
is done in Silver.
{% enddocs %}

{% docs brz_col_updated_at_en %}
Timestamp of the record's last modification in the source system.
Pivot column for Hive partitioning and the incremental filter.
{% enddocs %}

{% docs brz_col_loaded_at_en %}
Timestamp of when the row was loaded by dbt into the Bronze layer.
Allows distinguishing the source modification date (`updated_at`) from
the arrival date in the data lake.
{% enddocs %}

{% docs brz_col_partition_annee_en %}
Year extracted from `updated_at`. Hive partitioning column — always last
in the SELECT to respect Hive convention.
{% enddocs %}

{% docs brz_col_partition_mois_en %}
Month extracted from `updated_at` (1–12). Hive partitioning column.
{% enddocs %}

{% docs brz_col_partition_jour_en %}
Day extracted from `updated_at` (1–31). Hive partitioning column.
{% enddocs %}
