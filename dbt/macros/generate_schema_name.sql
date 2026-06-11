{#
  Unity Catalog override for generate_schema_name.

  dbt's default implementation concatenates target schema + custom schema:
    payments__payments
  That is wrong for Unity Catalog — the schema must be the exact name.

  This override returns custom_schema_name as-is, falling back to
  target.schema when no custom schema is set on a model.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
