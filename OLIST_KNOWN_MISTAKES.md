# Olist Dataset — Known Mistakes & How This Pipeline Handles Them

---

## Mistake 1 — `customer_id` vs `customer_unique_id`

**What goes wrong:**
Olist assigns a brand new `customer_id` for every order a customer places. Grouping by `customer_id` makes every customer look like a one-time buyer. Churn rate comes out at ~97% — completely wrong.

**Fix in this pipeline:**
`stg_customers.sql` retains `customer_unique_id` as the stable identifier. `int_customer_orders.sql` joins on `customer_unique_id` before any aggregation. Unity Catalog column documentation explicitly explains why.

---

## Mistake 2 — Including Non-Delivered Orders in Metrics

**What goes wrong:**
Including canceled, processing, and unavailable orders skews revenue totals, delivery time averages, and churn signals.

**Fix in this pipeline:**
`stg_orders.sql` adds an `is_delivered` flag. `int_orders_enriched.sql` filters `WHERE is_delivered = 1` before any metric calculation. No downstream model ever touches non-delivered orders.

---

## Mistake 3 — September/October 2018 Revenue Drop Misread as a Business Problem

**What goes wrong:**
Many analyses flag the sharp revenue drop in late 2018 as a business problem requiring investigation. The dataset simply ends there — it is not a real decline.

**Fix in this pipeline:**
The churn model uses `2018-10-17` as the explicit observation window end. The incremental mart uses this as its watermark. The interview narrative addresses it directly. No analyst reading this pipeline would misinterpret the drop.

---

## Mistake 4 — Geolocation Table Duplicate Explosion

**What goes wrong:**
The geolocation table has ~1 million rows for only ~19,000 unique zip codes. Joining naively causes row multiplication and inflated counts.

**Fix in this pipeline:**
No fix applied — and none attempted. Averaging latitude and longitude across duplicate zip entries is not meaningful and produces geographically incorrect centroids. The delivery performance mart uses `customer_state` from the customers table instead, which is accurate and requires no geolocation join. The geolocation table is ingested to bronze as-is for completeness but not used downstream.

---

## Mistake 5 — Duplicate Review IDs in `order_reviews`

**What goes wrong:**
The `order_reviews` table contains ~814 duplicate rows. Using them as-is inflates review counts and distorts average scores.

**Fix in this pipeline:**
`stg_order_reviews.sql` uses `ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY review_created_at DESC)` and keeps only the most recent review per order. Deduplication is explicit before any downstream join.

---

## Mistake 6 — Fabricating Profit Margins

**What goes wrong:**
Many analyses calculate profit margin by product category. The dataset has no cost-of-goods column — only `price` and `freight_value`. Any profit margin figure is fabricated.

**Fix in this pipeline:**
Not applicable. This pipeline never attempts profit margin. All metrics — revenue, order value, delivery time, churn probability — are derived only from columns that actually exist in the dataset.

---

## Mistake 7 — Multi-Seller Orders Causing Row Duplication

**What goes wrong:**
A single order in Olist can contain items from multiple sellers. Joining seller data at the order level without aggregating first inflates order counts and corrupts metrics.

**Fix in this pipeline:**
The `items` CTE in `int_orders_enriched.sql` aggregates all item-level data — including `min(seller_id) as primary_seller_id` — grouped by `order_id` before any join. No row explosion is possible. The deliberate choice of `min(seller_id)` is explicitly commented in the model.

---

## Mistake 8 — Negative Delivery Days from Bad Timestamps

**What goes wrong:**
Some orders have `delivered_at` before `order_purchased_at` due to data entry errors. This produces negative delivery times which corrupt delivery day averages and delay calculations.

**Fix in this pipeline:**
`int_orders_enriched.sql` nulls out invalid delivery times rather than filtering the order entirely — preserving the order for revenue calculations while excluding the bad metric:
```sql
case
    when datediff(o.delivered_at, o.order_purchased_at) < 0 then null
    else datediff(o.delivered_at, o.order_purchased_at)
end  as total_delivery_days
```

---

## Mistake 9 — Payment Row Duplication from Installments

**What goes wrong:**
Orders paid in installments or with multiple payment methods have multiple rows in `order_payments`. Joining without aggregating first causes row duplication and inflated order values.

**Fix in this pipeline:**
The `payments` CTE in `int_orders_enriched.sql` aggregates all payment rows — `sum(payment_value)`, `count(distinct payment_type)`, `max(payment_installments)` — grouped by `order_id` before joining to orders. No duplication is possible.

---