{{
    config(
        materialized = 'table'
        )
}}

with raw_products as (
    select distinct product_sku, product_category, product_size, product_style, asin_code from {{ ref('cleansed_sales')}}
)
SELECT
    row_number() over (order by product_sku, product_category, product_size, product_style, asin_code) as product_id,
    product_sku,
    product_category,
    product_size,
    product_style,
    asin_code
from raw_products
