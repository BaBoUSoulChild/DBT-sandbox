{#
  Override dbt par défaut : génère le nom de schéma/base de données Hive
  en respectant la convention <target_schema>_<custom_schema>.
  Ex : dev_sandbox_bronze, prod_sandbox_gold
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- set default_schema = target.schema -%}
  {%- if custom_schema_name is none -%}
    {{ default_schema }}
  {%- else -%}
    {{ default_schema }}_{{ custom_schema_name | trim }}
  {%- endif -%}
{%- endmacro %}


{#
  Construit le nom complet d'une table Hive : base.table
#}
{% macro hive_table_ref(schema_suffix, table_name) %}
  {{ target.schema }}_{{ schema_suffix }}.{{ table_name }}
{% endmacro %}
