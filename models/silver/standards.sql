{{ config(
    materialized='table'
) }}

with source as (

    select *
    from {{ ref('standards_stg') }}

),

exploded as (

    select
        _id,
        _data,
        created_at,
        updated_at,
        jsonb_array_elements_text(_data -> 'jurisdiction') as jurisdictions,
        jsonb_array_elements_text(_data -> 'business_size') as business_size,
        jsonb_array_elements_text(_data -> 'industries') as industries
    from source

),

parsed as (

    select
        _id,
        _data ->> 'type' as type,
        _data ->> 'name' as name,
        _data ->> 'icon' as icon,
        _data ->> 'no_of_employee' as no_of_employees,
        business_size,
        jurisdictions,
        _data ->> 'description' as description,
        _data ->> 'url' as url,
        (_data -> 'start_date')::text as start_date,
        (_data -> 'end_date')::text as end_date,
        (_data -> 'region')::text as region,
        (_data -> 'city')::text as city,
        (_data -> 'postal_code')::text as postal_code,
        (_data -> 'zip_code')::text as zip_code,
        (_data -> 'state')::text as state,
        _data ->> 'code' as code,
        _data ->> 'sub_type' as sub_type,
        _data ->> 'purpose' as purpose,
        _data ->> 'asset_base_value' as asset_base_value,
        _data ->> 'asset_base_currency' as asset_base_currency,
        _data ->> 'turn_over_value' as turn_over_value,
        _data ->> 'turn_over_currency' as turn_over_currency,
        _data ->> 'legal_status' as legal_status,
        _data ->> 'legal_status_year' as legal_status_year,
        _data ->> 'scope' as scope,
        _data ->> 'enactment_planned_enactment' as enactment_planned_enactment,
        industries,
        (_data -> 'county')::text as county,
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