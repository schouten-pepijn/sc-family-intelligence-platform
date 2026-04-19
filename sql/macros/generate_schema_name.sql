{% macro generate_schema_name(custom_schema_name, node) -%}
    {# Allow explicit per-model schema override while defaulting to target.
       Enables layered architecture (staging, marts) using dbt_project.yml config. #}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name }}
    {%- endif -%}
{%- endmacro %}
