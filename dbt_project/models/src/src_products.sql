with raw_products as (
    select distinct style, sku, category, size from AMAZON.RAW.RAW_SALES
)
SELECT
    row_number() over (partition by 1 order by sku) as product_id,
    style as product_style,
    sku as product_sku,
    category as product_category,
    size as product_size
from raw_products
