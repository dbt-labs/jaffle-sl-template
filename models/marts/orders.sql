{{
    config(
        materialized = 'table',
        unique_key = 'order_id'
    )
}}

with

orders as (

    select * from {{ ref('stg_orders') }}

    where
        true

        {% if is_incremental() %}

            and ordered_at >= (
                select max(ordered_at) as most_recent_record from {{ this }}
            )

        {% endif %}

)


select * from orders
