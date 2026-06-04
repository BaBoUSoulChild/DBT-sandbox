{# English translations of Gold doc blocks.
   To activate: replace doc('gld_commandes_par_jour') references in _schema.yml
   with doc('gld_commandes_par_jour_en'), etc.
#}

{% docs gld_commandes_par_jour_en %}
Daily order aggregates calculated from the Silver layer.

Available indicators: total and validated revenue excl. tax, order count,
distinct client count, average basket, cancellation count and rate.

**Mandatory deduplication:** Silver is multi-version (partitioned by `updated_at`).
Every Gold model starts with a `deduplicate()` CTE to keep only the most recent
version of each order before aggregating. Without this, a modified order would
be counted multiple times and all figures would be wrong.

Uses `date_commande` (stable business date from the source) as the aggregation
axis rather than `updated_at`.

In incremental mode, the model recalculates all days belonging to the current
month (and months covered by the lookback window) to guarantee aggregate
consistency even with late Silver arrivals. The Gold partition is at
`annee/mois` level, coarser than Bronze/Silver.

This model feeds the daily sales dashboard.
{% enddocs %}

{% docs gld_kpi_clients_en %}
Client KPIs calculated month by month from Silver orders and the Silver
client reference table.

For each `(id_client, month)` pair: revenue excl. tax, average basket,
order count, cancellation count, and first/last order dates of the month.
This model is the main source for loyalty reporting and client segmentation.

**Mandatory deduplication:** same as `gld_commandes_par_jour` — both
`slv_commandes` and `slv_clients` are deduplicated before the join and
aggregation.

In incremental mode, months affected by Silver updates are fully recalculated
to guarantee aggregate consistency.
{% enddocs %}

{% docs gld_col_ca_ht_total_en %}
Total revenue excluding tax for the day, in euros, across all statuses
(including cancelled or refunded orders).
{% enddocs %}

{% docs gld_col_ca_ht_valide_en %}
Revenue excluding tax for orders with status `VALIDE` only.
Main business indicator for tracking confirmed sales.
{% enddocs %}

{% docs gld_col_panier_moyen_ht_en %}
Average order amount excl. tax for the day, rounded to 2 decimal places.
{% enddocs %}

{% docs gld_col_taux_annulation_pct_en %}
Percentage of cancelled orders out of the day's total, rounded to 2 decimal
places. A spike in this indicator may signal an operational issue or a
failing promotional campaign.
{% enddocs %}

{% docs gld_col_date_reference_en %}
First day of the reference month for client KPI calculation (DATE_TRUNC MONTH).
Used as the temporal join key for BI tools.
{% enddocs %}
