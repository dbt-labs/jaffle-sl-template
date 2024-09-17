with source as (
    select * from {{ source('subscription-data', 'subscriptions') }}
)

select
    subscription_id,
    customer_id,
    plan_id,
    status,
    start_date::date as start_date,
    end_date::date as end_date,
    canceled_at::timestamp as canceled_at
from subscriptions