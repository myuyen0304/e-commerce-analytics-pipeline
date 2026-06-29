-- One row per product, with the English category name joined in.
with products as (
    select * from {{ source('olist_raw', 'products') }}
    qualify row_number() over (
        partition by product_id order by _airbyte_extracted_at desc
    ) = 1
),
translation as (
    select * from {{ source('olist_raw', 'category_translation') }}
)
select
    cast(p.product_id as string)                       as product_id,
    cast(p.product_category_name as string)            as product_category_name_pt,
    coalesce(
        cast(t.product_category_name_english as string),
        cast(p.product_category_name as string),
        'unknown'
    )                                                  as product_category_name,
    cast(p.product_weight_g as int)                    as product_weight_g
from products p
left join translation t
    on p.product_category_name = t.product_category_name
