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
    where code like '01-0010-0010-%'
    and code not in (
        '01-0010-0010-001',
        '01-0010-0010-002'
    )

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
                'code', code,
                'code_name', code_name,
                'rollup_qty', rollup_qty,
                'rollup_emissions', rollup_emissions,
                'dimensions', dimensions
            )
        ) as dimensions_and_codes

    from source
    group by
        company_code,
        site_code,
        reporting_year

)

select *
from aggregated
order by
    company_code,
    site_code,
    reporting_year