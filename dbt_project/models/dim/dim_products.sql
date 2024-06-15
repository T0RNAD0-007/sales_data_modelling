{{
    config(
        materialized = 'table'
        )
}}

with raw_products as (
    select distinct product_sku, product_category, product_size, product_style from {{ source('DEV_STAGE', 'cleansed_sales')}}
)
SELECT
    row_number() over (order by product_sku, product_category, product_size, product_style) as product_id,
    product_sku,
    product_category,
    product_size,
    product_style
from raw_products
