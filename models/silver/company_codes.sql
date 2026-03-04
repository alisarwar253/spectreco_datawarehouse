{{ config(
    materialized='table'
) }}

with source as (

    select *
    from {{ ref('company_codes_stg') }}

),

parsed as (

    select
        _id,
        (_data ->> 'internal_code_id')::text as internal_code_id,
        _data ->> 'category_id' as category_id,
        _data ->> 'company_id'as company_id,
        (_data -> 'site_code')::jsonb as site_code,
        (_data ->> 'isChecked')::boolean as is_checked,
        (_data ->> 'is_double')::boolean as is_double,
        to_timestamp(((_data -> 'createdAt' ->> '$date')::bigint) / 1000) as created_at,
        to_timestamp(((_data -> 'updatedAt' ->> '$date')::bigint) / 1000) as updated_at,
        current_timestamp as record_inserted_at

    from source

)

select * from parsed