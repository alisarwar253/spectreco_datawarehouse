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
        created_at,
        updated_at,
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
        created_at,
        updated_at,
        current_timestamp as record_processed_at

    from exploded

)

select * from parsed