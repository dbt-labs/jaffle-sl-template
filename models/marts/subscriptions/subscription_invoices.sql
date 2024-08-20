with source as (
    select * from {{ source('subscription-data', 'subscription_invoices') }}
)

select * from source