with source as (
    select * from {{ source('subscription-data', 'subscription_invoices') }}
)

select 
    invoice_id,
    subscription_id,
    period_start::date as period_start,
    period_end::date as period_end,
    total_amount,
    status,
    created_at::date as created_at,
    invoice_date::date as invoice_date,
from source