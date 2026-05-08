with source as (
    select * from {{ source('mongo_raw', 'cdata_yearly') }}
),

renamed as (
    select
        _id,
        replace(_doc, ': nan', ': null')::jsonb as _data,
        {{ mongo_date("replace(_doc, ': nan', ': null')::jsonb", "created_at") }} as created_at,
        {{ mongo_date("replace(_doc, ': nan', ': null')::jsonb", "updated_at") }} as updated_at,

        current_timestamp as dbt_seen_at
    from source
)

select * from renamed
