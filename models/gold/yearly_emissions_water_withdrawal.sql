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
        dimensions
    from {{ ref('cdata_yearly') }}
    where code = '01-0060-0030-001'

),

aggregated as (

    select
        company_code,
        site_code,
        reporting_year,
        sum(rollup_qty::numeric) as total_rollup_qty,
        sum(rollup_emissions::numeric) as total_rollup_emissions,

        -- JSON of contributing codes
        json_agg(
            json_build_object(
                'name', code_name,
                'value', rollup_qty,
                'emissions', rollup_emissions,
                'code', code,
                'level', 4,
                'children', coalesce(dimensions, '[]'::jsonb)
            )
        ) as level_4_children

    from source
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
                'value', total_rollup_qty,
                'emission', total_rollup_emissions,
                'code', '01-0060-0030',

                'children', level_4_children
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