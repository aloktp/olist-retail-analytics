with source as (

    select * 
    from {{ source('bronze', 'products') }}

),

cleaned as (

    select *
    from source

)

select * from cleaned