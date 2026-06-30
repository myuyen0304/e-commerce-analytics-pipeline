-- One row per customer_id. customer_unique_id is the real person across orders.
with source as (
    select * from {{ source('olist_raw', 'customers') }}
)
select
    cast(customer_id as string)            as customer_id,
    cast(customer_unique_id as string)     as customer_unique_id,
    cast(customer_zip_code_prefix as int)  as customer_zip_code_prefix,
    cast(customer_city as string)          as customer_city,
    cast(customer_state as string)         as customer_state
from source
qualify row_number() over (
    partition by customer_id order by _dlt_load_id desc
) = 1
