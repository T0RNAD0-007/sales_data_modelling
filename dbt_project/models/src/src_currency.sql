with raw_curreny as (
    select distinct currency, ship_country from AMAZON.RAW.RAW_SALES
)
SELECT
    row_number() over (partition by 1 order by currency) as currency_id,
    currency as currency_symbol
    ship_country as currency_country
from raw_curreny
