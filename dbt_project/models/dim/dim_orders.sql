{{
    config(
        materialized = 'table'
        )
}}

with raw_orders as (
    select distinct order_id, asin_code, order_status, order_fulfilled_by, b2b from {{ source('DEV_STAGE', 'cleansed_sales')}}
)
SELECT
    row_number() over (partition by 1 order by order_id, asin_code, order_status, order_fulfilled_by, b2b) as order_id,
    order_id as order_id_number,
    order_status,
    order_fulfilled_by,
    asin_code,
    b2b
from raw_orders
