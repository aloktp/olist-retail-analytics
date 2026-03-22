with source as (

    select * 
    from {{ source('bronze', 'sellers') }}

),

cleaned as (

    select
        seller_id,
        seller_zip_code_prefix as seller_zip,
        seller_city,
        seller_state

    from source
    where seller_id is not null

)

select * from cleaned