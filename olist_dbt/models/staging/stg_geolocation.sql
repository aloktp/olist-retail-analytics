with source as (

    select * from {{ source('bronze', 'geolocation') }}

),

cleaned as (

    select
        geolocation_zip_code_prefix as zip_code,
        geolocation_lat as latitude,
        geolocation_lng as longitude,
        geolocation_city,
        geolocation_state

    from source

)

select * from cleaned