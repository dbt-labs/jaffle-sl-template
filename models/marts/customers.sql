with

customers as (

    select * from {{ ref('stg_customers') }}

),

orders_mart as (

    select * from {{ ref('orders') }}

),

order_items_mart as (

    select * from {{ ref('order_items') }}
),

order_summary as (

    select
        customer_id,

        count(distinct om.order_id) as count_lifetime_orders,
        count(distinct om.order_id) > 1 as is_repeat_buyer,
        min(om.ordered_at) as first_ordered_at,
        max(om.ordered_at) as last_ordered_at,
        sum(oi.subtotal) as lifetime_spend_pretax,
        sum(om.order_total) as lifetime_spend

    from orders_mart om
    
    left join order_items_mart oi on om.order_id = oi.order_id
    
    group by 1

),

joined as (

    select
        customers.*,
        order_summary.count_lifetime_orders,
        order_summary.first_ordered_at,
        order_summary.last_ordered_at,
        order_summary.lifetime_spend_pretax,
        order_summary.lifetime_spend,

        case
            when order_summary.is_repeat_buyer then 'returning'
            else 'new'
        end as customer_type

    from customers

    left join order_summary
        on customers.customer_id = order_summary.customer_id

)

select * from joined
