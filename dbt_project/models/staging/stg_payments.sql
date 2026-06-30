-- One row per payment record (grain: order_id + payment_sequential).
with source as (
    select * from {{ source('olist_raw', 'order_payments') }}
)
select
    cast(order_id as string)             as order_id,
    cast(payment_sequential as int)      as payment_sequential,
    cast(payment_type as string)         as payment_type,
    cast(payment_installments as int)    as payment_installments,
    cast(payment_value as decimal(12, 2)) as payment_value
from source
qualify row_number() over (
    partition by order_id, payment_sequential order by _dlt_load_id desc
) = 1
