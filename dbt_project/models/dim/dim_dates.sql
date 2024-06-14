{{
    config(
        materialized = 'table'
        )
}}

with raw_dates as (
    select distinct date from {{ source('DEV_STAGE', 'cleansed_sales')}}
)
SELECT
    row_number() over (partition by 1 order by date) as date_id,
    date as date,
    month(date) as month,
    year(date) as year,
    quarter(date) as quater,
    dayname(date) as day_of_week,
    CASE WHEN upper(dayname(date)) in ('SAT','SUN') THEN 1 ELSE 0 END as holiday
from raw_dates
