with source as (
    select * from {{ source('subscription-data', 'plans') }}
)

select * from source