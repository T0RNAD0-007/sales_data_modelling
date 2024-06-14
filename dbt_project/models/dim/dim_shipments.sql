{{
    config(
        materialized = 'table'
        )
}}

with raw_orders as (
    select distinct fulfilment, ship_service_level, sales_channel, courier_status from {{ source('DEV_STAGE', 'cleansed_sales')}}
)
SELECT
    row_number() over (order by fulfilment, ship_service_level, sales_channel, courier_status) as shipment_id,
    fulfilment,
    ship_service_level,
    sales_channel,
    courier_status
from raw_orders
