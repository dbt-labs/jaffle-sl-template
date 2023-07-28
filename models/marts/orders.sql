{{
    config(
        materialized = 'table',
        unique_key = 'order_id'
    )
}}


with orders as (
    
    select * from {{ ref('stg_orders')}}

),

order_items as (
    
    select * from {{ ref('stg_order_items')}}

),

products as (

    select * from {{ ref('stg_products') }}
),

supplies as (

    select * from {{ ref('stg_supplies') }}

),


order_items_summary as (

    select

        order_items.order_id,

        sum(supplies.supply_cost) as order_cost,
        sum(is_food_item) as count_food_items,
        sum(is_drink_item) as count_drink_items


    from order_items

    left join supplies on order_items.product_id = supplies.product_id
    left join products on order_items.product_id = products.product_id

    group by 1

),


final as (
    select

        orders.*,
        count_food_items > 0 as is_food_order,
        count_drink_items > 0 as is_drink_order,
        order_cost

    from orders
    
    left join order_items_summary on orders.order_id = order_items_summary.order_id
)

select * from final