{{
    config(
        materialized = 'table'
        )
}}

with raw_dates as (
    select distinct date from {{ ref('cleansed_sales')}}
)
SELECT
    row_number() over (order by date) as date_id,
    date,
    month(date) as month,
    year(date) as year,
    quarter(date) as quater,
    dayname(date) as day_of_week,
    CASE WHEN upper(dayname(date)) in ('SAT','SUN') THEN TRY_TO_BOOLEAN(TRUE) ELSE TRY_TO_BOOLEAN(FALSE) END as is_holiday
from raw_dates
