-- GOLD: revenue & order volume per calendar month.
-- Revenue = sum of item prices on non-cancelled orders.
with orders as (
    select *
    from {{ ref('stg_orders') }}
    where order_status not in ('canceled', 'unavailable')
      and order_purchase_timestamp is not null
),
items as (
    select * from {{ ref('stg_order_items') }}
)
select
    date_trunc('month', o.order_purchase_timestamp)         as order_month,
    count(distinct o.order_id)                              as n_orders,
    count(*)                                                as n_items,
    sum(i.price)                                            as total_revenue,
    sum(i.freight_value)                                    as total_freight,
    round(sum(i.price) / count(distinct o.order_id), 2)    as avg_order_value
from orders o
join items i on o.order_id = i.order_id
group by 1
order by 1
