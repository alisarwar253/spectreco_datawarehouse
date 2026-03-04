{{ config(
    materialized='table'
) }}

with source as (

    select *
    from {{ ref('company_sites_stg') }}

),

parsed as (

    select
        _id,
        (_data ->> 'id')::text as id,
        _data ->> 'internal_site_code' as internal_site_code,
        _data ->> 'site_type' as site_type,
        _data ->> 'zipcode' as zipcode,
        _data ->> 'site_name' as site_name,
        _data ->> 'country' as country,
        _data ->> 'state' as state,
        _data ->> 'city' as city,
        _data ->> 'town' as town,
        _data ->> 'addressline1' as addressline1,
        _data ->> 'addressline2' as addressline2,
        _data ->> 'parentSiteCode' as parent_site_code,
        (_data ->> 'company_id')::text as company_id,
        (_data ->> 'lca')::text as lca,
        _data ->> 'ownership_status' as ownership_status,
        _data ->> 'ownership' as ownership,
        _data ->> 'lca_category' as lca_category,
        _data ->> 'total_area' as total_area,
        _data ->> 'life_span' as life_span,
        _data ->> 'covered_area' as covered_area,
        _data ->> 'project_drawing' as project_drawing,
        (_data ->> 'is_reporting')::boolean as is_reporting,
        _data ->> 'area_unit' as area_unit,
        _data ->> 'control' as control,
        to_timestamp(((_data -> 'construction_start_date' ->> '$date')::bigint) / 1000) as construction_start_date,
        to_timestamp(((_data -> 'commissioning_date' ->> '$date')::bigint) / 1000) as commissioning_date,
        to_timestamp(((_data -> 'created_at' ->> '$date')::bigint) / 1000) as created_at,
        to_timestamp(((_data -> 'updated_at' ->> '$date')::bigint) / 1000) as updated_at,
        current_timestamp as record_inserted_at

    from source

)

select * from parsed