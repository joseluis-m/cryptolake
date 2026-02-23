{#
  Macro: create_table_as (override for Spark + Iceberg)

  The Iceberg REST catalog ignores the LOCATION set on the namespace.
  Without this macro, all dbt tables would land in the default bucket
  (cryptolake-bronze). We inject an explicit LOCATION pointing to the
  correct bucket based on the model's schema.

  Example for a model in schema "gold":
    CREATE OR REPLACE TABLE gold.dim_coins
    USING iceberg
    LOCATION 's3://cryptolake-gold/dim_coins'
    AS SELECT ...
#}

{% macro spark__create_table_as(temporary, relation, compiled_code) -%}
  {%- set bucket = 'cryptolake-' ~ relation.schema -%}

  create or replace table {{ relation }}
  using iceberg
  location 's3://{{ bucket }}/{{ relation.identifier }}'
  as
  {{ compiled_code }}
{%- endmacro %}
