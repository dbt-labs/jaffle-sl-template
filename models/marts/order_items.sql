{{
    config(
        materialized = 'table',
        unique_key = 'order_item_id'
    )
}}

with order_items as (

    select * from {{ ref('stg_order_items') }}

),


orders as (
    
    select * from {{ ref('stg_orders')}}
),

products as (

    select * from {{ ref('stg_products') }}

),


final as (
    select
        order_items.*,
        orders.ordered_at,
        products.product_price as subtotal,
        products.is_food_item,
        products.is_drink_item
    from order_items

    left join products on order_items.product_id = products.product_id
    -- left join order_supplies_summary on order_items.order_id = order_supplies_summary.product_id
    left join orders on order_items.order_id  = orders.order_id
)

select * from final