{{
    config(
        materialized = 'table'
        )
}}

with sales as (
    select sales_transaction_id, location_id, product_id, sales_quantity from {{ source('DEV_FACT', 'fact_sales')}}
),
location as (
    select location_id, state, city from {{ source('DEV_DIM', 'dim_locations')}}
),
product as (
    select product_id, product_style, product_category from {{ source('DEV_DIM', 'dim_products')}}
)
select
    sa.sales_transaction_id,
    ps.product_style as product_style,
    ps.product_category as product_category,
    ls.state as sales_state,
    ls.city as sales_city,
    sa.sales_quantity
FROM sales sa
JOIN location ls on sa.location_id = ls.location_id
JOIN product ps on sa.product_id = ps.product_id