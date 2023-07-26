{{
    config(
        materialized = 'table',
        unique_key = 'order_item_id'
    )
}}

with order_items as (

    select * from {{ ref('stg_order_items') }}

),

-- supplies as (

--     select * from {{ ref('stg_supplies') }}

-- ),

orders as (
    select * from {{ ref('stg_orders')}}
),

products as (

    select * from {{ ref('stg_products') }}

),

-- order_supplies_summary as (

--     select

--         order_items.order_id,

--         sum(supplies.supply_cost) as order_cost

--     from order_items

--     left join supplies on order_items.product_id = supplies.product_id

--     group by 1

-- ),

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