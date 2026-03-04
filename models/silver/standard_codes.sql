{{ config(
    materialized='table'
) }}

with source as (

    select *
    from {{ ref('standard_codes_stg') }}

),

parsed as (

    select
        _id,
        (_data ->> 'code')::text as code,
        _data ->> 'name' as name,
        _data ->> 'description' as description,
        (_data ->> 'status')::boolean as status,
        _data ->> 'type_detail' as type_detail,
        _data ->> 'type' as type,
        (_data -> 'internal_code')::jsonb as internal_code,
        _data ->> 'function' as function,
        _data ->> 'custom_1' as custom_1,
        _data ->> 'custom_2' as custom_2,
        _data ->> 'custom_3' as custom_3,
        _data ->> 'custom_4' as custom_4,
        to_timestamp(((_data -> 'created_at' ->> '$date')::bigint) / 1000) as created_at,
        to_timestamp(((_data -> 'updated_at' ->> '$date')::bigint) / 1000) as updated_at,
        current_timestamp as record_inserted_at

    from source

)

select * from parsed