with timestamps as (

    select 
        'categories_stg' as table_name,
        max(updated_at) as latest_updated_at
    from {{ ref('categories_stg') }}

    union all

    select 
        'cdata_bi_annual_stg',
        max(updated_at)
    from {{ ref('cdata_bi_annual_stg') }}

    union all

    select 
        'cdata_bi_annual_forecast_stg',
        max(updated_at)
    from {{ ref('cdata_bi_annual_forecast_stg') }}

    union all

    select 
        'cdata_month_stg',
        max(updated_at)
    from {{ ref('cdata_month_stg') }}

    union all

    select 
        'cdata_month_forecast_stg',
        max(updated_at)
    from {{ ref('cdata_month_forecast_stg') }}

    union all

    select 
        'cdata_quarter_stg',
        max(updated_at)
    from {{ ref('cdata_quarter_stg') }}

    union all

    select 
        'cdata_quarter_forecast_stg',
        max(updated_at)
    from {{ ref('cdata_quarter_forecast_stg') }}

    union all

    select 
        'cdata_stg',
        max(updated_at)
    from {{ ref('cdata_stg') }}

    union all

    select 
        'cdata_yearly_stg',
        max(updated_at)
    from {{ ref('cdata_yearly_stg') }}

    union all

    select 
        'cdata_yearly_baseline_stg',
        max(updated_at)
    from {{ ref('cdata_yearly_baseline_stg') }}

    union all

    select 
        'cdata_yearly_forecast_stg',
        max(updated_at)
    from {{ ref('cdata_yearly_forecast_stg') }}

    union all

    select 
        'cdata_yearly_target_stg',
        max(updated_at)
    from {{ ref('cdata_yearly_target_stg') }}

    union all

    select 
        'companies_stg',
        max(updated_at)
    from {{ ref('companies_stg') }}

    union all

    select 
        'company_sites_stg',
        max(updated_at)
    from {{ ref('company_sites_stg') }}

    union all

    select 
        'company_frameworks_stg',
        max(updated_at)
    from {{ ref('company_frameworks_stg') }}

    union all

    select 
        'standards_stg',
        max(updated_at)
    from {{ ref('standards_stg') }}

    union all

    select 
        'company_codes_stg',
        max(updated_at)
    from {{ ref('company_codes_stg') }}

    union all

    select 
        'standard_codes_stg',
        max(updated_at)
    from {{ ref('standard_codes_stg') }}

)

select *
from timestamps
where latest_updated_at is not null
order by latest_updated_at desc
limit 1