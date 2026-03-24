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
    where code in (
        '01-0030-0010-013',
		'01-0030-0010-014',
		'01-0030-0010-015',
		'01-0030-0010-016'	
    )

),

aggregated as (

    select
        company_code,
        site_code,
        reporting_year,
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
                'emission', total_rollup_emissions,
                'code', '01-0010-0020',
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