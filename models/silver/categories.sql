{{ config(
    materialized='table'
) }}

with source as (

    select *
    from {{ ref('categories_stg') }}

),

exploded as (

    select
        _id,
        _data,
        created_at,
        updated_at,
        jsonb_array_elements_text(_data -> 'scope') as scope,
        jsonb_array_elements_text(_data -> 'countries') as countries
    from source

),

parsed as (

    select
        _id,
        _data ->> 'name' as name,
        _data ->> 'parent_id' as parent_id,
        _data ->> 'code' as code,
        _data ->> 'description' as description,
        (_data ->> 'status')::boolean as status,
        scope,
        countries,
        _data ->> 'icon' as icon,
        _data ->> 'custom_1' as custom_1,
        _data ->> 'custom_2' as custom_2,
        _data ->> 'custom_3' as custom_3,
        _data ->> 'custom_4' as custom_4,
        created_at,
        updated_at,
        current_timestamp as record_processed_at

    from exploded

)

select * from parsed