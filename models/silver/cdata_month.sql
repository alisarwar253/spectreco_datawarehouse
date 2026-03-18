{{ config(
    materialized='table'
) }}

with source as (

    select *
    from {{ ref('cdata_month_stg') }}

),

base as (

    select
        _id,
        _data ->> 'company_code'                        as company_code,
        (_data -> 'internal_code_id' ->> '$oid')        as internal_code_id,
        (_data -> 'category_id' ->> '$oid')             as category_id,
        (_data ->> 'month_index')::text                  as month_index,
        _data ->> 'month'                               as month,
        (_data ->> 'reporting_year')::text              as reporting_year,
        (_data ->> 'type_year')::text                   as type_year,
        _data ->> 'unit'                                as unit,
        _data ->> 'standard_unit'                       as standard_unit,
        _data ->> 'value'                               as value,
        _data ->> 'qty'                                 as qty,
        _data ->> 'standard_qty'                        as standard_qty,
        _data ->> 'total_emissions'                     as total_emissions,
        _data ->> 'standard_emissions'                  as standard_emissions,
        _data ->> 'site_code'                            as site_code,
        _data ->> 'scope'                               as scope,
        _data ->> 'code'                                as code,
        _data ->> 'code_name'                           as code_name,
        _data ->> 'currency'                            as currency,
        _data ->> 'description'                         as description,
        _data ->> 'ref_table'                           as ref_table,
        (_data ->> 'is_forecast')::boolean              as is_forecast,
        (_data ->> 'rollup_processed')::boolean         as rollup_processed,
        nullif(_data ->> 'rollup_emissions','')::numeric as rollup_emissions,
        nullif(_data ->> 'rollup_qty','')::numeric       as rollup_qty,
        nullif(_data ->> 'rollup_value','')::numeric     as rollup_value,

        -- rollup_processed_at safe handling like created_at
        case
            when jsonb_typeof(_data -> 'rollup_processed_at') = 'string'
                then (_data ->> 'rollup_processed_at')::timestamptz
            when jsonb_typeof(_data -> 'rollup_processed_at' -> '$date') = 'number'
                then to_timestamp(((_data -> 'rollup_processed_at' ->> '$date')::bigint)/1000) at time zone 'UTC'
            else null
        end as rollup_processed_at,

        -- created_at safe handling
        case
            when jsonb_typeof(_data -> 'created_at' -> '$date') = 'number'
                then to_timestamp(((_data -> 'created_at' ->> '$date')::bigint)/1000) at time zone 'UTC'
            else (_data -> 'created_at' ->> '$date')::timestamptz
        end as created_at,

        current_timestamp as record_inserted_at,

        -- SAFE dimension array
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
    internal_code_id,
    category_id,
    month_index,
    month,
    reporting_year,
    type_year,
    qty,
    standard_qty,
    unit,
    standard_unit,
    value,
    currency,
    total_emissions,
    standard_emissions,
    site_code,
    scope,
    code,
    code_name,
    description,
    ref_table,
    is_forecast,
    rollup_processed,
    rollup_emissions,
    rollup_qty,
    rollup_value,
    dimensions,
    rollup_processed_at,
    created_at,
    record_inserted_at

from base