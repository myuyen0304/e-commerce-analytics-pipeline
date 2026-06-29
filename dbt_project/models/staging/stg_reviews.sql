-- One row per (review_id, order_id). review_score drives product quality metrics.
with source as (
    select * from {{ source('olist_raw', 'order_reviews') }}
)
select
    cast(review_id as string)                  as review_id,
    cast(order_id as string)                   as order_id,
    cast(review_score as int)                  as review_score,
    cast(review_creation_date as timestamp)    as review_creation_date
from source
qualify row_number() over (
    partition by review_id, order_id order by _airbyte_extracted_at desc
) = 1
