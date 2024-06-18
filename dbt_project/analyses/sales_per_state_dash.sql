with sales as (
    select * from {{ ref('sales_per_state')}}
)
select * FROM sales