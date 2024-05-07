select
    count(distinct order_id) as count_orders,
    date_trunc('week',ordered_at) as orderd_at__week
from {{ref('orders')}}
group by 2