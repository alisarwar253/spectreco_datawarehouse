{% macro generate_schema_name(custom_schema_name, node) %}

    {% set auto_create_schemas = ['dw_stg', 'dw_silver', 'dw_gold'] %}

    {% if custom_schema_name in auto_create_schemas %}
        {{ log("Creating schema if not exists: " ~ custom_schema_name, info=True) }}
        {{ run_query("CREATE SCHEMA IF NOT EXISTS " ~ custom_schema_name) }}
    {% else %}
        {{ log("Skipping schema creation for: " ~ custom_schema_name, info=True) }}
    {% endif %}

    {{ return(custom_schema_name) }}
{% endmacro %}