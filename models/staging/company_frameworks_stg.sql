with source as (
    select * from {{ source('mongo_raw', 'company_frameworks') }}
),

renamed as (
    select
        _id,
        _doc::jsonb as _data
    from source
)

select * from renamed
