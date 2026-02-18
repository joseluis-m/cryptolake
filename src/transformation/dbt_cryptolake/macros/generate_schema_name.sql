{#
  ============================================================
  Macro: generate_schema_name
  ============================================================
  Sobreescribe el comportamiento por defecto de dbt para
  nombrar schemas.

  Comportamiento por defecto de dbt:
    target.schema = "gold", custom_schema = "staging"
    → genera: "gold_staging"  (¡no es lo que queremos!)

  Nuestro comportamiento:
    custom_schema = "staging" → genera: "staging"
    custom_schema = null      → genera: target.schema ("gold")

  Esto hace que las tablas de staging vayan al namespace
  "cryptolake.staging" y las de marts al namespace "cryptolake.gold".
  ============================================================
#}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
