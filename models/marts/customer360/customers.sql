{{
  config(
    materialized = "view",
  )
}}

select * from {{ source('semantic_layer_imports', 'customers') }}

