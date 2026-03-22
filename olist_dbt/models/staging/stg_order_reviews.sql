with source as (

    select * 
    from {{ source('bronze', 'order_reviews') }}

),

cleaned as (

    select
        review_id,
        order_id,
        cast(review_score as int) as review_score,

        case when cast(review_score as int) <= 2 then 1 else 0 end as is_low_satisfaction,

        to_timestamp(review_creation_date, 'd/M/yyyy H:mm') as review_created_at

    from source
    where order_id is not null
      and review_score is not null

)

select * from cleaned