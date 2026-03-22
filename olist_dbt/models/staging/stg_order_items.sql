with source as (

    select * 
    from {{ source('bronze', 'order_items') }}

),

cleaned as (

    select
        order_id,
        order_item_id,
        product_id,
        seller_id,

        cast(price as double) as item_price,
        cast(freight_value as double) as freight_value,
        cast(price as double) + cast(freight_value as double) as item_total_value,

        to_timestamp(shipping_limit_date, 'd/M/yyyy H:mm') as shipping_limit_at

    from source
    where order_id is not null

)

select * from cleaned