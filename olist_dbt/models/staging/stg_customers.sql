with source as (

    select * 
    from {{ source('bronze', 'customers') }}

),

cleaned as (

    select
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix as customer_zip,
        customer_city,
        customer_state

    from source
    where customer_id is not null

)

select * from cleaned