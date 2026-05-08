{{ config(
    materialized='table'
) }}

with source as (
    select *
    from {{ ref('cdata_yearly_baseline_stg') }}
),

base as (
    select
        _id,
        _data ->> 'company_code'       as company_code,
        _data ->> 'year'            as year,
        _data ->> 'semesters' as semesters,
        (_data ->> 'type_year')::text   as type_year,
        (_data ->> 'reporting_year')::text as reporting_year,
        (_data ->> 'qty')::text     as qty,
        (_data ->> 'standard_qty')::text as standard_qty,
        (_data ->> 'value')::text   as value,
        (_data ->> 'total_emissions')::text as total_emissions,
        (_data ->> 'standard_emissions')::text as standard_emissions,
        _data ->> 'site_code'          as site_code,
        (_data -> 'internal_code_id' ->> '$oid') as internal_code_id,
        (_data -> 'category_id' ->> '$oid') as category_id,
        _data ->> 'scope'              as scope,
        _data ->> 'code'               as code,
        _data ->> 'code_name'          as code_name,
        _data ->> 'currency'           as currency,
        _data ->> 'unit'               as unit,
        _data ->> 'standard_unit'      as standard_unit,
        _data ->> 'description'        as description,
        _data ->> 'ref_table'          as ref_table,
        _data ->> 'note'               as note,
        (_data ->> 'is_forecast')::boolean as is_forecast,
        (_data ->> 'rollup_processed')::boolean as rollup_processed,
        nullif(_data ->> 'rollup_qty','')::numeric as rollup_qty,
        nullif(_data ->> 'rollup_value','')::numeric as rollup_value,
        nullif(_data ->> 'rollup_emissions','')::numeric as rollup_emissions,

        case
            when jsonb_typeof(_data -> 'dimension') = 'array'
            then _data -> 'dimension'
            else '[]'::jsonb
        end as dimensions,

        -- safe rollup_processed_at
        case
            when jsonb_typeof(_data -> 'rollup_processed_at' -> '$date') = 'number'
                then to_timestamp(((_data -> 'rollup_processed_at' ->> '$date')::bigint)/1000) at time zone 'UTC'
            when _data ? 'rollup_processed_at'
                then (_data ->> 'rollup_processed_at')::timestamptz
            else null
        end as rollup_processed_at,

        created_at,
        updated_at,


        current_timestamp as record_processed_at

    from source
)

select *
from base