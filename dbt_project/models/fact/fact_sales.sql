{{
    config(
        materialized = 'table'
        )
}}

with cleansed_sales as (
    select * from {{ source('DEV_STAGE', 'cleansed_sales')}}
)
SELECT
    sales_transaction_id,
    sales_amount,
    sales_quantity,
    dc.currency_id as currency_id,
    dd.date_id as date_id,
    dl.location_id as location_id,
    do.order_id as order_id,
    dp.product_id as product_id,
    ds.shipment_id as shipment_id
FROM cleansed_sales as cs
JOIN {{source('DEV_DIM', 'dim_currency')}} as dc on cs.currency = dc.currency_symbol AND cs.ship_country = dc.currency_country
JOIN {{source('DEV_DIM', 'dim_dates')}} dd on cs.date = dd.date
JOIN {{source('DEV_DIM', 'dim_locations')}} as dl on cs.ship_city = dl.city AND cs.ship_state = dl.state AND
    cs.ship_postal_code = dl.postal_code AND cs.ship_country = dl.country
JOIN {{source('DEV_DIM', 'dim_orders')}} as do on cs.order_id = do.ordered_id AND cs.asin_code = do.asin_code AND
    cs.order_status = do.order_status AND cs.order_fulfilled_by = do.order_fulfilled_by AND cs.b2b = do.b2b
JOIN {{source('DEV_DIM', 'dim_products')}} as dp on cs.product_sku = dp.product_sku AND cs.product_category = dp.product_category
    AND cs.product_size = dp.product_size
JOIN {{source('DEV_DIM', 'dim_shipments')}} as ds on cs.fulfilment = ds.fulfilment AND cs.ship_service_level = ds.ship_service_level
    AND cs.sales_channel = ds.sales_channel AND cs.courier_status = ds.courier_status
