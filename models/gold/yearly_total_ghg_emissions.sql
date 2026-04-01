{{ config(
    materialized='materialized_view'
) }}

with scope1 as (
    select
        company_code,
        site_code,
        reporting_year,
        sum((elem->>'emission')::numeric) as emissions
    from {{ ref('yearly_emissions_scope_1') }},
    lateral json_array_elements(actual_data) as elem
    group by company_code, site_code, reporting_year
),

scope2 as (
    -- scope 2 stores emission in 'value' at the parent level (no 'emission' key)
    select
        company_code,
        site_code,
        reporting_year,
        sum((elem->>'value')::numeric) as emissions
    from {{ ref('yearly_emissions_energy_purchased_scope_2') }},
    lateral json_array_elements(actual_data) as elem
    group by company_code, site_code, reporting_year
),

scope3 as (
    select
        company_code,
        site_code,
        reporting_year,
        sum((elem->>'emission')::numeric) as emissions
    from {{ ref('yearly_emissions_scope_3') }},
    lateral json_array_elements(actual_data) as elem
    group by company_code, site_code, reporting_year
),

combined as (
    select * from scope1
    union all
    select * from scope2
    union all
    select * from scope3
)

select
    company_code,
    site_code,
    reporting_year,
    sum(emissions) as total_ghg_emissions
from combined
group by
    company_code,
    site_code,
    reporting_year
order by
    company_code,
    site_code,
    reporting_year