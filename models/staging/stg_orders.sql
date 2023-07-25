{{
    config(
        materialized = 'table',
        unique_key = 'order_id'
    )
}}

with

source as (

    select * from {{ source('ecom', 'raw_orders') }}

    -- data runs to 2026, truncate timespan to desired range,
    -- current time as default
    -- where ordered_at <= {{ var('truncate_timespan_to') }}

),

renamed as (

    select

        ----------  ids
        id as order_id,
        store_id as location_id,
        customer as customer_id,

        ---------- properties
        (order_total / 100.0) as order_total,
        (tax_paid / 100.0) as tax_paid,

        ---------- timestamps
        ordered_at

    from source

),

label_first_orders as (

  select
    *,
{# TODO make this more crossdb #}
    case when
      first_value(order_id) over (partition by customer_id order by ordered_at) = order_id
    then 1 else 0 end as is_first_order

  from renamed

)

select * from label_first_orders
