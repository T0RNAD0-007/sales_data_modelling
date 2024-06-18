{{
    config(
        materialized = 'table'
        )
}}

with cleansed_sales as (
    select * from {{ ref('cleansed_sales')}}
)
SELECT
    sales_transaction_id,
    sales_amount,
    sales_quantity,
    promotion_applied as sales_promotion_applied,
    dd.date_id as date_id,
    dl.location_id as location_id,
    do.order_id as order_id,
    dp.product_id as product_id,
    ds.shipment_id as shipment_id
FROM cleansed_sales as cs
JOIN {{ref('dim_dates')}} dd on cs.date = dd.date
JOIN {{ref('dim_locations')}} as dl on COALESCE(cs.ship_city,'1') = COALESCE(dl.city,'1')
    AND COALESCE(cs.ship_state,'1') = COALESCE(dl.state,'1') AND COALESCE(cs.ship_postal_code,'1') = COALESCE(dl.postal_code,'1')
    AND COALESCE(cs.ship_country,'1') = COALESCE(dl.country,'1')
JOIN {{ref('dim_orders')}} as do on COALESCE(cs.order_id,'1') = COALESCE(do.ordered_id,'1')
    AND COALESCE(cs.order_status,'1') = COALESCE(do.order_status,'1')
    AND COALESCE(cs.order_fulfilled_by,'1') = COALESCE(do.order_fulfilled_by,'1') AND COALESCE(cs.b2b,'1') = COALESCE(do.b2b,'1')
JOIN {{ref('dim_products')}} as dp on COALESCE(cs.product_sku,'1') = COALESCE(dp.product_sku,'1') AND
    COALESCE(cs.product_style,'1') = COALESCE(dp.product_style,'1') AND COALESCE(cs.asin_code,'1') = COALESCE(dp.asin_code,'1')
    AND COALESCE(cs.product_category,'1') = COALESCE(dp.product_category,'1') AND COALESCE(cs.product_size,'1') = COALESCE(dp.product_size,'1')
JOIN {{ref('dim_shipments')}} as ds on COALESCE(cs.fulfilment,'1') = COALESCE(ds.fulfilment,'1')
    AND COALESCE(cs.ship_service_level,'1') = COALESCE(ds.ship_service_level,'1')
    AND COALESCE(cs.sales_channel,'1') = COALESCE(ds.sales_channel,'1') AND COALESCE(cs.courier_status,'1') = COALESCE(ds.courier_status,'1')
