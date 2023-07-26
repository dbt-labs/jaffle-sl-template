{{
    config(
        materialized = 'table',
        unique_key = 'order_id'
    )
}}


with orders as (
    select * from {{ ref('stg_orders')}}
),

fct_order_items as (
    select * from {{ ref('fct_order_items')}}
),

order_items_summary as (

    select

        fct_order_items.order_id,

        sum(is_food_item) as count_food_items,
        sum(is_drink_item) as count_drink_items
    from fct_order_items
    
    group by 1

),

final as (
    select

        orders.*,
        count_food_items > 0 as is_food_order,
        count_drink_items > 0 as is_drink_order
    from orders
    left join order_items_summary on orders.order_id = order_items_summary.order_id

)

select * from final