{{
    config(
        materialized = 'table'
        )
}}

with sales as (
    select date_id, sales_amount, sales_transaction_id, sales_quantity from {{ ref('fact_sales')}}
),
date as (
    select date_id, quater, year, month, is_holiday, date from {{ ref('dim_dates')}}
)
select
    sales_amount, sales_transaction_id, sales_quantity, ds.date_id as date_id, quater, year, month, is_holiday, date
FROM sales sa
JOIN date ds on sa.date_id = ds.date_id