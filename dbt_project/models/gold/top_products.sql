-- GOLD: products ranked by revenue, with units sold and average review score.
with orders as (
    select *
    from {{ ref('stg_orders') }}
    where order_status not in ('canceled', 'unavailable')
),
items as (
    select * from {{ ref('stg_order_items') }}
),
products as (
    select * from {{ ref('stg_products') }}
),
review_per_order as (
    select order_id, avg(review_score) as avg_review
    from {{ ref('stg_reviews') }}
    group by order_id
),
item_facts as (
    select
        i.product_id,
        i.order_id,
        i.price,
        r.avg_review
    from items i
    join orders o on i.order_id = o.order_id
    left join review_per_order r on i.order_id = r.order_id
)
select
    f.product_id,
    p.product_category_name,
    count(*)                          as units_sold,
    count(distinct f.order_id)        as n_orders,
    sum(f.price)                      as total_revenue,
    -- NOTE: review score is per-order; orders with multiple item lines weight it
    -- more than once. Good enough as a learning metric; refine if needed.
    round(avg(f.avg_review), 2)       as avg_review_score
from item_facts f
left join products p on f.product_id = p.product_id
group by 1, 2
order by total_revenue desc
