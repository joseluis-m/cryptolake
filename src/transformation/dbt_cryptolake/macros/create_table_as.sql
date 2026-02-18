{#
  ============================================================
  Macro: create_table_as (override para Spark + Iceberg)
  ============================================================
  Problema: El catálogo REST de Iceberg (tabulario) ignora el
  LOCATION del namespace. Sin esta macro, todas las tablas que
  dbt crea irían al bucket por defecto (cryptolake-bronze).

  Solución: Inyectamos LOCATION explícito en cada CREATE TABLE
  apuntando al bucket correcto basándonos en el schema del modelo.

  Ejemplo para un modelo en schema "gold":
    CREATE OR REPLACE TABLE gold.dim_coins
    USING iceberg
    LOCATION 's3://cryptolake-gold/dim_coins'
    AS SELECT ...
  ============================================================
#}

{% macro spark__create_table_as(temporary, relation, compiled_code) -%}
  {# Construimos el nombre del bucket a partir del schema #}
  {%- set bucket = 'cryptolake-' ~ relation.schema -%}

  create or replace table {{ relation }}
  using iceberg
  location 's3://{{ bucket }}/{{ relation.identifier }}'
  as
  {{ compiled_code }}
{%- endmacro %}
