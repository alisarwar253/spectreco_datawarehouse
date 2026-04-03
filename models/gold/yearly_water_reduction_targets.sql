{{ config(
    materialized='materialized_view'
) }}

with source as (

    select
        company_code,
        site_code,
        reporting_year,
        code,
        code_name,
        rollup_qty,
        rollup_emissions,
        dimensions::jsonb as dimensions
    from {{ ref('cdata_yearly') }}
    where code = '01-0060-0050-001'

),

normalized as (

    select
        company_code,
        site_code,
        reporting_year,
        code,
        code_name,
        rollup_qty,
        rollup_emissions,

        (
            select jsonb_agg(
                jsonb_build_object(
                    'name', coalesce(child->>'value1', child->>'name'),
                    'code_name', code_name,
                    'value', case when child->>'qty' ~ '^[0-9.]+$' then (child->>'qty')::numeric else null end,
                    'units', child->>'unit',
                    'code', child->>'code',
                    'year', reporting_year,

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

        sum(rollup_qty::numeric) as total_value,

        json_agg(
            json_build_object(
                'name', code_name,
                'code_name', code_name,
                'value', rollup_qty,
                'code', code,
                'year', reporting_year,
                'children', coalesce(normalized_dimensions, '[]'::jsonb)
            )
        ) as children

    from normalized
    group by
        company_code,
        site_code,
        reporting_year

),

final as (

    select
        company_code,
        site_code,
        reporting_year,

        json_agg(
            json_build_object(
                'name', reporting_year,
                'code_name', 'Water Reduction Targets',
                'value', total_value,
                'code', '01-0060-0050',
                'year', reporting_year,
                'children', children
            )
        ) as actual_data

    from aggregated
    group by
        company_code,
        site_code,
        reporting_year
)

select *
from final
order by
    company_code,
    site_code,
    reporting_year