{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name == 'staging' or custom_schema_name == 'intermediate' -%}
        silver
    {%- elif custom_schema_name == 'marts' -%}
        gold
    {%- elif custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name }}
    {%- endif -%}
{%- endmacro %}
