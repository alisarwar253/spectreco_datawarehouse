with source as (
    select * from {{ source('mongo_raw', 'cdata') }}
),

renamed as (
    select
        _id,
        _doc::jsonb as _data,
        {{ mongo_date("_doc::jsonb", "createdAt") }} as created_at,
        {{ mongo_date("_doc::jsonb", "updatedAt") }} as updated_at,

        current_timestamp as record_processed_at
    from source
)

select * from renamed
