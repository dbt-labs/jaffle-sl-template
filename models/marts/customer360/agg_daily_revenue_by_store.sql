select
    sum(distinct product_price) as sum_product_price,
    date_trunc('day',ordered_at) as orderd_at__day
from {{ref('order_items')}}
group by 2