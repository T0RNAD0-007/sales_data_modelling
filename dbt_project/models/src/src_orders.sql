with raw_orders as (
    select distinct order_id, status, fulfilled_by from AMAZON.RAW.RAW_SALES
)
SELECT
    order_id as order_id,
    status as order_status,
    fulfilled_by
from raw_orders
