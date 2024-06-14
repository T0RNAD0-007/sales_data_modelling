{{
    config(
        materialized = 'table'
        )
}}

with raw_products as (
    select distinct product_sku, product_category, product_size from {{ source('DEV_STAGE', 'cleansed_sales')}}
)
SELECT
    row_number() over (partition by 1 order by product_sku, product_category, product_size) as product_id,
    product_sku,
    product_category,
    product_size
from raw_products
