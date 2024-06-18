{{
    config(
        materialized = 'table'
        )
}}

with raw_orders as (
    select distinct order_id, order_status, order_fulfilled_by, b2b, promotional_code from {{ ref('cleansed_sales')}}
)
SELECT
    row_number() over (order by order_id, order_status, order_fulfilled_by, b2b) as order_id,
    order_id as ordered_id,
    order_status,
    order_fulfilled_by,
    b2b,
    dc.promotional_id
from raw_orders rw
JOIN {{ref('dim_coupons')}} dc
ON COALESCE(rw.promotional_code,'1') = COALESCE(dc.promotional_code,'1')