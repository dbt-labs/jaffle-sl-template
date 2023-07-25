{{
    config(
        materialized='table'
    )
}}

with

customers as (

    select * from {{ ref('stg_customers') }}

)

select * from customers
