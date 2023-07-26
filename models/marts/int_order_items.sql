{{
    config(
        materialized = 'table',
        unique_key = 'order_id'
    )
}}

with order_items as (

    select * from {{ ref('stg_order_items') }}

),

supplies as (

    select * from {{ ref('stg_supplies') }}

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
        products.product_price as subtotal
        supplies.supply_cost as order_cost
    from order_items

    left join products on order_items.product_id = products.product_id
    left join supplies on order_items.product_id = supplies.product_id
    left join orders on order_items.order_id  = orders.order_id

    group by 1
)