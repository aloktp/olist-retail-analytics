with source as (

    select * 
    from {{ source('bronze', 'order_payments') }}

),

cleaned as (

    select
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        cast(payment_value as double) as payment_value

    from source
    where order_id is not null

)

select * from cleaned