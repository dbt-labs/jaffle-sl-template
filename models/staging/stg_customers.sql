with

source as (

    select * from {{ source('ecom', 'raw_customers') }}

),

renamed as (

    select

        ----------  ids
        id as customer_id,

        ---------- text
        name as customer_name,
        gender as customer_gender,
        birthday as customer_birthday

    from source

)

select * from renamed
