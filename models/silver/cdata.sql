{{ config(
    materialized='table'
) }}

with source as (

    select *
    from {{ ref('cdata_stg') }}

),

base as (
    select
        _id,
        _data ->> 'company_code' as company_code,
        _data ->> 'company_name' as company_name,
        (_data -> 'internal_code_id' ->> '$oid')as internal_code_id,
        (_data -> 'category_id' ->> '$oid') as category_id,
        (_data ->> 'standard_emissions')::text as standard_emissions,
        (_data ->> 'standard_qty')::text as standard_qty,
        _data ->> 'standard_unit' as standard_unit,
        _data ->> 'site_code' as site_code,
        _data ->> 'scope' as scope,
        (_data ->> 'qty')::text as qty,
        _data ->> 'unit' as unit,
        _data ->> 'currency' as currency,
        (_data ->> 'value')::text as value,
        _data ->> 'month' as month,
        _data ->> 'quarter' as quarter,
        _data ->> 'semi_annual' as semi_annual,
        _data ->> 'description' as description,
        _data ->> 'parent_id' as parent_id,
        _data ->> 'type' as type,
        _data ->> 'type_year' as type_year,
        (_data ->> 'total_emissions')::text as total_emissions,

        _data ->> 'url' as url,
        (_data ->> 'is_aggregated')::boolean as is_aggregated,

        case
            when jsonb_typeof(_data -> 'createdAt' -> '$date') = 'number'
                then to_timestamp(((_data -> 'createdAt' ->> '$date')::bigint) / 1000) at time zone 'UTC'
            else (_data -> 'createdAt' ->> '$date')::timestamptz
        end as created_at,

        case
            when jsonb_typeof(_data -> 'updatedAt' -> '$date') = 'number'
                then to_timestamp(((_data -> 'updatedAt' ->> '$date')::bigint) / 1000) at time zone 'UTC'
            else (_data -> 'updatedAt' ->> '$date')::timestamptz
        end as updated_at,

        current_timestamp as record_inserted_at,
        -- safe dimension array (empty array if missing or not an array)
        case 
            when jsonb_typeof(_data -> 'dimension') = 'array' 
            then _data -> 'dimension'
            else '[]'::jsonb
        end as dimensions

    from source
)

select
    _id,
    company_code,
    company_name,
    internal_code_id,
    category_id,
    standard_emissions,
    standard_qty,
    standard_unit,
    site_code,
    scope,
    qty,
    unit,
    currency,
    value,
    month,
    quarter,
    semi_annual,
    description,
    parent_id,
    type,
    type_year,
    total_emissions,
    url,
    is_aggregated,
    dimensions,
    created_at,
    updated_at,
    record_inserted_at

from base