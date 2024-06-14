{{
    config(
        materialized = 'table'
        )
}}

with raw_locations as (
    select distinct ship_city, ship_state, ship_country, ship_postal_code from {{ source('DEV_STAGE', 'cleansed_sales')}}
)
SELECT
    row_number() over (order by ship_city, ship_state, ship_postal_code, ship_country) as location_id,
    ship_city as city,
    ship_state as state,
    ship_country as country,
    ship_postal_code as postal_code
from raw_locations
