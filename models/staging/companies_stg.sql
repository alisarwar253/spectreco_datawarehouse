with source as (
    select * from {{ source('mongo_raw', 'companies') }}
),

renamed as (
    select
        _id,
        _doc::jsonb as _data,
        {{ mongo_date("_doc::jsonb", "created_at") }} as created_at,
        {{ mongo_date("_doc::jsonb", "updated_at") }} as updated_at,

        current_timestamp as dbt_seen_at
    from source
)

select * from renamed
