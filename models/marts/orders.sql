{{
    config(
        materialized = 'table',
        unique_key = 'order_id'
    )
}}

with

supplies as (
 
  select
    supply_id,
    supply_cost 

  from {{ ref('stg_supplies') }}

),

products as (

  select
    product_id,
    product_price 

  from {{ ref('stg_products') }}

),

orders as (

    select *

    from {{ ref('stg_orders') }}

    where
        true

        {% if is_incremental() %}

            and ordered_at >= (
                select max(ordered_at) as most_recent_record from {{ this }}
            )

        {% endif %}

),

joined as (

  select 
    orders.*,
    products.product_price,
    supplies.supply_cost
  
  from orders
  
  left join supplies on orders.supply_id = supplies.supply_id
  left join products on orders.product_id = products.product_id

)

select * from joined
