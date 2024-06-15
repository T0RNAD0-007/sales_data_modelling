{{
    config(
        materialized = 'table'
        )
}}

with sales as (
    select date_id, sales_amount, sales_transaction_id, sales_quantity from {{ source('DEV_FACT', 'fact_sales')}}
),
date as (
    select date_id, quater, year, month, holiday, date from {{ source('DEV_DIM', 'dim_dates')}}
)
select
    sales_amount, sales_transaction_id, sales_quantity, ds.date_id as date_id, quater, year, month, holiday, date
FROM sales sa
JOIN date ds on sa.date_id = ds.date_id