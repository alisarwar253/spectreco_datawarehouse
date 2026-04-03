{{ config(
    materialized='materialized_view'
) }}

with renewable as (

    select
        company_code,
        site_code,
        reporting_year,
        quarter,
        months,
        actual_data->0 as renewable_obj
    from {{ ref('quarterly_energy_renewable_sources') }}

),

non_renewable as (

    select
        company_code,
        site_code,
        reporting_year,
        quarter,
        months,
        actual_data->0 as non_renewable_obj
    from {{ ref('quarterly_energy_non_renewable_sources') }}

),

combined as (

    select
        coalesce(r.company_code, n.company_code)     as company_code,
        coalesce(r.site_code, n.site_code)           as site_code,
        coalesce(r.reporting_year, n.reporting_year) as reporting_year,
        coalesce(r.quarter, n.quarter)             as quarter,
        coalesce(r.months, n.months)               as months,
        coalesce((r.renewable_obj->>'value')::numeric, 0)
            + coalesce((n.non_renewable_obj->>'value')::numeric, 0) as total_value,

        json_build_array(r.renewable_obj, n.non_renewable_obj) as children

    from renewable r
    full outer join non_renewable n
        on  r.company_code   = n.company_code
        and r.site_code      = n.site_code
        and r.reporting_year = n.reporting_year
        and r.quarter        = n.quarter
        and r.months         = n.months
),

final as (

    select
        company_code,
        site_code,
        reporting_year,
        quarter,
        months,
        json_agg(
            json_build_object(
                'name',      reporting_year,
                'code_name', 'Total Energy Produced',
                'value',     total_value,
                'code',      '01-0030-0010-031',
                'year',      reporting_year,
                'quarter',   quarter,
                'months',    months,
                'children',  children
            )
        ) as actual_data

    from combined
    group by
        company_code,
        site_code,
        reporting_year,
        quarter,
        months

)

select *
from final
order by
    company_code,
    site_code,
    reporting_year,
    quarter,
    months