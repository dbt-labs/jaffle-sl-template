with source as (
    select * from {{ source('subscription-data', 'customers') }}
)

select * from source