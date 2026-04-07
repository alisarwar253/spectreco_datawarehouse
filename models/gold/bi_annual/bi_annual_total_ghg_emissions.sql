{{ config(
    materialized='materialized_view'
) }}

with scope1 as (
    select
        company_code,
        site_code,
        reporting_year,
        semester,
        'scope1' as scope,
        (elem->>'emission')::numeric as emission,
        (elem->>'value')::numeric as value
    from {{ ref('bi_annual_emissions_scope_1') }},
    lateral json_array_elements(actual_data) as elem
),

scope2 as (
    select
        company_code,
        site_code,
        reporting_year,
        semester,
        'scope2' as scope,
        (elem->>'value')::numeric as emission,
        null::numeric as value
    from {{ ref('bi_annual_emissions_energy_purchased_scope_2') }},
    lateral json_array_elements(actual_data) as elem
),

scope3 as (
    select
        company_code,
        site_code,
        reporting_year,
        semester,
        'scope3' as scope,
        (elem->>'emission')::numeric as emission,
        (elem->>'value')::numeric as value
    from {{ ref('bi_annual_emissions_scope_3') }},
    lateral json_array_elements(actual_data) as elem
),

combined as (
    select * from scope1
    union all
    select * from scope2
    union all
    select * from scope3
),

scope_level as (
    select
        company_code,
        site_code,
        reporting_year,
        semester,
        scope,

        sum(emission) as scope_emission_sum,
        sum(coalesce(value, 0)) as scope_value_sum,

        jsonb_agg(
            jsonb_strip_nulls(
                jsonb_build_object(
                    'emission', emission,
                    'value', value
                )
            )
        ) as raw_contributions

    from combined
    group by
        company_code,
        site_code,
        reporting_year,
        semester,
        scope
),

final as (
    select
        company_code,
        site_code,
        reporting_year,
        semester,
        jsonb_build_object(
            'total_emissions', sum(scope_emission_sum),
            'total_value', sum(scope_value_sum),
            'scopes',
            jsonb_agg(
                jsonb_build_object(
                    'scope', scope,
                    'emission_sum', scope_emission_sum,
                    'value_sum', scope_value_sum,
                    'contributions', raw_contributions,
                    'year', reporting_year,
                    'semester', semester
                )
            )
        ) as actual_data

    from scope_level
    group by
        company_code,
        site_code,
        reporting_year,
        semester
)

select
    company_code,
    site_code,
    reporting_year,
    semester,
    actual_data
from final
order by
    company_code,
    site_code,
    reporting_year,
    semester