-- CTE 1: Filter only delivered orders
-- We start with orders because this is the main thing in the final gold dataset.
with orders as (

    select *
    from {{ ref('stg_orders') }}
    where is_delivered = 1   -- keep only completed orders

),

-- CTE 2: Aggregate order items
-- Each order can have multiple items → we summarize them at order_id level.
items as (

    select
        order_id,

        -- number of items in the order
        count(order_item_id) as item_count,

        -- sum of product prices
        sum(item_price) as items_subtotal,

        -- total freight charged
        sum(freight_value) as freight_total,

        -- total gross value (items + freight)
        sum(item_total_value) as order_gross_value,

        -- pick one seller (most orders have 1 seller)
        min(seller_id) as primary_seller_id

    from {{ ref('stg_order_items') }}
    group by order_id
),

-- CTE 3: Aggregate payments
-- Orders can have multiple payments (e.g., split payments).
payments as (

    select
        order_id,

        -- total amount paid
        sum(payment_value) as total_payment_value,

        -- number of distinct payment methods used
        count(distinct payment_type) as payment_method_count,

        -- highest number of installments used
        max(payment_installments) as max_installments

    from {{ ref('stg_order_payments') }}
    group by order_id
),

-- CTE 4: Get the latest review per order
-- Some orders have multiple reviews → we keep the most recent one.
reviews as (

    select 
        order_id, 
        review_score, 
        is_low_satisfaction
    from (
        select *,
               -- rank reviews by recency within each order
               row_number() over (
                   partition by order_id 
                   order by review_created_at desc
               ) as rn
        from {{ ref('stg_order_reviews') }}
    )
    where rn = 1   -- keep only the latest review
),

-- CTE 5: Final dataset combining all features
final as (

    select
        o.order_id,
        o.customer_id,
        o.order_status,
        o.order_purchased_at,
        o.delivered_at,
        o.estimated_delivery_at,

        -- DELIVERY METRICS
        -- difference between actual and estimated delivery
        datediff(o.delivered_at, o.estimated_delivery_at) as delivery_delay_days,

        -- flag if delivery was late
        case 
            when o.delivered_at > o.estimated_delivery_at then 1 
            else 0 
        end as is_late_delivery,

        -- total days from purchase to delivery
        datediff(o.delivered_at, o.order_purchased_at) as total_delivery_days,

        -- ORDER VALUE METRICS
        i.item_count,
        i.order_gross_value,
        p.total_payment_value,
        p.payment_method_count,
        p.max_installments,
        i.primary_seller_id as seller_id,

        -- REVIEW METRICS
        r.review_score,
        r.is_low_satisfaction

    from orders o
    left join items i on o.order_id = i.order_id
    left join payments p on o.order_id = p.order_id
    left join reviews r on o.order_id = r.order_id
)

-- Final output
select * 
from final;
