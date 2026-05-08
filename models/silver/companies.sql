{{ config(
    materialized='table'
) }}

with source as (

    select *
    from {{ ref('companies_stg') }}

),

parsed as (

    select
        _id,
        (_data ->> 'id')::text as id,
        _data ->> 'company_name' as company_name,
        _data ->> 'country_incorporation' as country_incorporation,
        _data ->> 'legal_status' as legal_status,
        (_data ->> 'revenue')::bigint as revenue,
        _data ->> 'currency' as currency,
        (_data ->> 'no_of_employees')::bigint as no_of_employees,
        _data ->> 'month' as month,
        _data ->> 'reporting_frequency' as reporting_frequency,
        _data ->> 'ecnomic_size' as ecnomic_size,
        _data ->> 'reporting_level' as reporting_level,
        _data ->> 'engine_sync_id' as engine_sync_id,
        _data ->> 'web_url' as web_url,
        _data ->> 'company_logo' as company_logo,
        _data ->> 'operational_countries' as operational_countries,
        _data ->> 'date_format' as date_format,
        _data ->> 'num_format' as num_format,
        (_data ->> 'is_completed')::boolean as is_completed,
        _data ->> 'jurisdiction' as jurisdiction,
        _data ->> 'company_code' as company_code,
        _data ->> 'region' as region,
        _data ->> 'emission_standard' as emission_standard,
        (_data ->> 'theme')::text as theme,
        (_data ->> 'incorporation_year')::text as incorporation_year,
        created_at,
        updated_at,
        current_timestamp as record_processed_at
    from source

)

select * from parsed