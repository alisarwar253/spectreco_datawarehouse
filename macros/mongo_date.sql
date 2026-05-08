{% macro mongo_date(json_col, field_name) %}
    case
        when jsonb_typeof({{ json_col }} -> '{{ field_name }}' -> '$date') = 'number'
            then to_timestamp((({{ json_col }} -> '{{ field_name }}' ->> '$date')::bigint) / 1000) at time zone 'UTC'
        when {{ json_col }} -> '{{ field_name }}' ->> '$date' is not null
            then ({{ json_col }} -> '{{ field_name }}' ->> '$date')::timestamptz
        else null
    end
{% endmacro %}