{{ config(
    materialized='table'
) }}

with source as (

    select *
    from {{ ref('cdata_bi_annual_stg') }}

),

base as (

    select
        _id,
        _data ->> 'company_code'                        as company_code,
        _data ->> 'semester'                            as semester,
        (_data ->> 'semester_index')::text              as semester_index,
        _data ->> 'quarters'                            as quarters,
        (_data ->> 'reporting_year')::text              as reporting_year,
        (_data ->> 'type_year')::text                   as type_year,
        (_data ->> 'qty')::text                         as qty,
        (_data ->> 'standard_qty')::text                as standard_qty,
        (_data ->> 'value')::text                       as value,
        (_data ->> 'total_emissions')::text             as total_emissions,
        (_data ->> 'standard_emissions')::text          as standard_emissions,
        _data ->> 'site_code'                           as site_code,
        (_data -> 'internal_code_id' ->> '$oid')        as internal_code_id,
        (_data -> 'category_id' ->> '$oid')             as category_id,
        _data ->> 'scope'                               as scope,
        _data ->> 'code'                                as code,
        _data ->> 'code_name'                           as code_name,
        _data ->> 'currency'                            as currency,
        _data ->> 'unit'                                as unit,
        _data ->> 'standard_unit'                       as standard_unit,
        _data ->> 'description'                         as description,
        _data ->> 'ref_table'                           as ref_table,

        (_data ->> 'is_forecast')::boolean              as is_forecast,
        (_data ->> 'rollup_processed')::boolean         as rollup_processed,

        -- Rollup values as strings
        nullif(_data ->> 'rollup_emissions','')::numeric            as rollup_emissions,
        nullif(_data ->> 'rollup_qty','')::numeric                  as rollup_qty,
        nullif(_data ->> 'rollup_value','')::numeric                as rollup_value,

        -- rollup_processed_at safe cast
        case
            -- case 1: object with $date number
            when jsonb_typeof(_data -> 'rollup_processed_at') = 'object'
                and jsonb_typeof(_data -> 'rollup_processed_at' -> '$date') = 'number'
                then to_timestamp(
                        ((_data -> 'rollup_processed_at' ->> '$date')::bigint) / 1000
                    ) at time zone 'UTC'

            -- case 2: object with $date string
            when jsonb_typeof(_data -> 'rollup_processed_at') = 'object'
                and jsonb_typeof(_data -> 'rollup_processed_at' -> '$date') = 'string'
                then (_data -> 'rollup_processed_at' ->> '$date')::timestamptz

            -- case 3: direct ISO string
            when jsonb_typeof(_data -> 'rollup_processed_at') = 'string'
                then (_data ->> 'rollup_processed_at')::timestamptz

            else null
        end as rollup_processed_at,

        -- created_at safe handling
        case
            when jsonb_typeof(_data -> 'created_at' -> '$date') = 'number'
                then to_timestamp(((_data -> 'created_at' ->> '$date')::bigint) / 1000) at time zone 'UTC'
            else (_data -> 'created_at' ->> '$date')::timestamptz
        end as created_at,

        current_timestamp as record_inserted_at,

        -- SAFE dimension array
        case
            when jsonb_typeof(_data -> 'dimension') = 'array'
            then _data -> 'dimension'
            else '[]'::jsonb
        end as dimension_array

    from source
),

-- explode dimension safely
dimension_exploded as (

    select
        b.*,
        dim_elem
    from base b
    left join lateral (
        select elem as dim_elem
        from jsonb_array_elements(b.dimension_array) elem
        where jsonb_typeof(elem) = 'object'
    ) t on true

),

children_exploded as (

    select
        d.*,
        elem as child_elem
    from dimension_exploded d
    left join lateral (
        select elem
        from jsonb_array_elements(
            case
                when jsonb_typeof(d.dim_elem -> 'children') = 'array'
                    then d.dim_elem -> 'children'
                else '[]'::jsonb
            end
        ) elem
        where jsonb_typeof(elem) = 'object'
    ) t on true

)

select

    _id,
    company_code,
    internal_code_id,
    category_id,
    semester,
    semester_index,
    quarters,
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

    -- Dimension level (strings)
    dim_elem ->> 'dimension'        as dimension_code,
    dim_elem ->> 'value1'           as dimension_value1,
    dim_elem ->> 'qty'              as dimension_qty,
    dim_elem ->> 'value'            as dimension_value,
    dim_elem ->> 'unit'             as dimension_unit,
    dim_elem ->> 'currency'         as dimension_currency,
    dim_elem ->> 'emissions'        as dimension_emissions,

    child_elem ->> 'technical_name' as children_technical_name,
    child_elem ->> 'value' as children_value,

    rollup_processed_at,
    created_at,
    record_inserted_at

from children_exploded