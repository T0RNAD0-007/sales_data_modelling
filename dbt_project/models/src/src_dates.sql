with raw_dates as (
    select distinct try_to_date(date,'MM-DD-YY') as date from AMAZON.RAW.RAW_SALES
)
SELECT
    row_number() over (partition by 1 order by date) as date_id,
    date as date,
    month(date) as month,
    year(date) as year,
    quarter(date) as quater,
    dayofweek(date) as day_of_week
from raw_dates
