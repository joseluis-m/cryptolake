{#
  Macro: generate_schema_name

  Overrides dbt's default schema naming behavior.

  Default dbt behavior:
    target.schema = "gold", custom_schema = "staging"
    -> generates: "gold_staging" (not what we want)

  Our behavior:
    custom_schema = "staging" -> generates: "staging"
    custom_schema = null      -> generates: target.schema ("gold")

  This routes staging models to "cryptolake.staging" and
  marts models to "cryptolake.gold".
#}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
