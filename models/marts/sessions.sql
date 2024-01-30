with dates as (
    select
        distinct ordered_at::date as date
    from {{ref('orders')}}
)

select
    dates.date,
    random()*100 as sessions
from dates