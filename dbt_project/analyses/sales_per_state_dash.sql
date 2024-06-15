with sales as (
    select * from {{ source('DEV_MART', 'sales_per_state')}}
)
select * FROM sales