{{ config(
    materialized='materialized_view'
) }}

with source as (

    select
        company_code,
        site_code,
        reporting_year,
        month,
        code,
        code_name,
        rollup_qty,
        rollup_emissions,
        dimensions::jsonb as dimensions
    from {{ ref('cdata_month') }}
    where code = '01-0030-0010-029'

),

normalized as (

    select
        company_code,
        site_code,
        reporting_year,
        month,
        code,
        code_name,
        rollup_qty,
        rollup_emissions,

        (
            select jsonb_agg(
                jsonb_build_object(
                    -- Parent level mappings
                    'name', coalesce(child->>'value1', child->>'name'),
                    'code_name', code_name,
                    'value', case when child->>'qty' ~ '^[0-9.]+$' then (child->>'qty')::numeric else null end,
                    'emission', case when child->>'emissions' ~ '^[0-9.]+$' then (child->>'emissions')::numeric else null end,
                    'units', child->>'unit',
                    'code', child->>'code',
                    'year', reporting_year,
                    'month', month,
                    -- Children remain same, only rename technical_name → name
                    'children',
                        (
                            select jsonb_agg(
                                (gc - 'technical_name') || jsonb_build_object('name', gc->>'technical_name')
                            )
                            from jsonb_array_elements(coalesce(child->'children','[]'::jsonb)) gc
                        )
                )
            )
            from jsonb_array_elements(coalesce(dimensions, '[]'::jsonb)) child
        ) as normalized_dimensions

    from source
),

aggregated as (

    select
        company_code,
        site_code,
        reporting_year,
        month,
        sum(rollup_qty::numeric) as total_value,
        sum(rollup_emissions::numeric) as total_emission,

        json_agg(
            json_build_object(
                'name', code_name,
                'code_name', code_name,
                'value', rollup_qty,
                'emission', rollup_emissions,
                'code', code,
                'year', reporting_year,
                'month', month,
                'children', coalesce(normalized_dimensions, '[]'::jsonb)
            )
        ) as children

    from normalized
    group by
        company_code,
        site_code,
        reporting_year,
        month

),

final as (

    select
        company_code,
        site_code,
        reporting_year,
        month,
        json_agg(
            json_build_object(
                'name', reporting_year,
                'code_name', 'Non Fossil Fuel Waste',
                'value', total_value,
                'emission', total_emission,
                'code', '01-0030-0010-029',
                'year', reporting_year,
                'month', month,
                'children', children
            )
        ) as actual_data

    from aggregated
    group by
        company_code,
        site_code,
        reporting_year,
        month
)

select *
from final
order by
    company_code,
    site_code,
    reporting_year,
    month