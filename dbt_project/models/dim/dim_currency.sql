{{
    config(
        materialized = 'table'
        )
}}

with raw_currency as (
    select distinct currency, ship_country from {{ source('DEV_STAGE', 'cleansed_sales')}}
)
SELECT
    row_number() over (order by currency, ship_country) as currency_id,
    currency as currency_symbol,
    ship_country as currency_country
from raw_currency
