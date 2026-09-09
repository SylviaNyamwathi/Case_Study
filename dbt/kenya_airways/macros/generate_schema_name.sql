{#
    Custom schema naming.

    dbt's default is <target_schema>_<custom_schema>, which produces
    `gold_marts` / `gold_staging`. For a warehouse where the layer names are the
    contract that BI tools bind to, the custom schema should win outright in
    production, while dev stays namespaced so two developers never collide.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- elif target.name == 'prod' or target.name == 'redshift' -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
