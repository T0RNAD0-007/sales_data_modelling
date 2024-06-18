{{
    config(
        materialized = 'table'
        )
}}

with sales as (
    select sales_quantity, sales_transaction_id,sales_amount, location_id from {{ ref('fact_sales')}}
),
location as (
    select location_id, state from {{ ref('dim_locations')}}
)
select
    ls.state as sales_state,
    sa.sales_amount as sales_per_state,
    sales_quantity,
    sales_transaction_id
FROM sales sa
JOIN location ls on sa.location_id = ls.location_id