{{ config(
    materialized='table'
) }}

with source as (

    select *
    from {{ ref('company_frameworks_stg') }}

),

exploded as (

    select
        _id,
        _data,
        jsonb_array_elements_text(_data -> 'regulation') as regulation,
        jsonb_array_elements_text(_data -> 'standards') as standards,
        jsonb_array_elements_text(_data -> 'frameworks') as frameworks
    from source

),

parsed as (

    select
        _id,
        regulation,
        standards,
        frameworks,
        _data ->> 'company_code' as company_code,
        to_timestamp(((_data -> 'createdAt' ->> '$date')::bigint) / 1000) as created_at,
        to_timestamp(((_data -> 'updatedAt' ->> '$date')::bigint) / 1000) as updated_at,
        current_timestamp as record_inserted_at

    from exploded

)

select * from parsed