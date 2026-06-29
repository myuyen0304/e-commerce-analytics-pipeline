-- GOLD: customers ranked by total spend (by real person = customer_unique_id).
-- Includes recency/frequency/monetary building blocks for a future RFM model.
with orders as (
    select *
    from {{ ref('stg_orders') }}
    where order_status not in ('canceled', 'unavailable')
),
items as (
    select * from {{ ref('stg_order_items') }}
),
customers as (
    select * from {{ ref('stg_customers') }}
),
order_revenue as (
    select
        o.order_id,
        o.customer_id,
        o.order_purchase_timestamp,
        sum(i.price) as order_revenue
    from orders o
    join items i on o.order_id = i.order_id
    group by 1, 2, 3
)
select
    c.customer_unique_id,
    count(distinct orv.order_id)               as n_orders,
    sum(orv.order_revenue)                     as total_spend,
    round(avg(orv.order_revenue), 2)           as avg_order_value,
    max(orv.order_purchase_timestamp)          as last_order_at
from order_revenue orv
join customers c on orv.customer_id = c.customer_id
group by 1
order by total_spend desc
