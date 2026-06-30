-- One row per item line within an order (grain: order_id + order_item_id).
-- price + freight_value are the basis for all revenue metrics.
with source as (
    select * from {{ source('olist_raw', 'order_items') }}
)
select
    cast(order_id as string)         as order_id,
    cast(order_item_id as int)       as order_item_id,
    cast(product_id as string)       as product_id,
    cast(seller_id as string)        as seller_id,
    cast(price as decimal(12, 2))    as price,
    cast(freight_value as decimal(12, 2)) as freight_value
from source
qualify row_number() over (
    partition by order_id, order_item_id order by _dlt_load_id desc
) = 1
