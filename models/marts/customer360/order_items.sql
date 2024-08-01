{{
  config(
    materialized = "view",
  )
}}

select * from {{ source('semantic_layer_imports', 'order_items') }}

