-- STEP 1: Create a CTE called "source"
-- This simply loads the raw table so we can transform it cleanly in the next step.
/*
with source as (

    select * 
    from workspace.bronze.orders

),
*/

with source as (

    select * 
    from {{ source('bronze', 'orders') }}

),

-- STEP 2: Create a second CTE called "cleaned"
-- This is where all cleaning, type conversion, and feature creation happens.
cleaned as (

    select
        -- Keep the key identifiers
        order_id,
        customer_id,
        order_status,

        -- Convert string timestamps into real timestamp data types
        -- This allows sorting by date, filtering by date, date differences and use time functions etc.
        to_timestamp(order_purchase_timestamp, 'd/M/yyyy H:mm') as order_purchased_at,
        to_timestamp(order_approved_at, 'd/M/yyyy H:mm') as order_approved_at,
        to_timestamp(order_delivered_carrier_date, 'd/M/yyyy H:mm') as shipped_at,
        to_timestamp(order_delivered_customer_date, 'd/M/yyyy H:mm') as delivered_at,
        to_timestamp(order_estimated_delivery_date, 'd/M/yyyy H:mm') as estimated_delivery_at,

        -- Create a binary flag (1/0) for delivered orders
        -- CASE WHEN does NOT affect timestamps or other columns.
        -- It only creates a new column called "is_delivered".
        case 
            when order_status = 'delivered' then 1 
            else 0 
        end as is_delivered

    from source
)

-- STEP 3: Output the cleaned dataset
select * 
from cleaned;
