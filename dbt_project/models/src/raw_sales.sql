{{
    config(
        materialized = 'view',
        on_schema_change = 'fail',
        )
}}

with raw_sales as (
    select * from {{ source('RAW','raw_sales')}}
)
SELECT
    index,
    order_id,
    date,
    status,
    fulfilment,
    sales_channel,
    ship_service_level,
    style,
    sku,
    category,
    size,
    asin,
    courier_status,
    qty,
    currency,
    amount,
    ship_city,
    ship_state,
    ship_postal_code,
    ship_country,
    promotion_ids,
    b2b,
    fulfilled_by,
    unknown_flag
from raw_sales
