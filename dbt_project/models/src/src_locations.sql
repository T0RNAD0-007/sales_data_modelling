with raw_locations as (
    select distinct ship_city, ship_state, ship_country, ship_postal_code from AMAZON.RAW.RAW_SALES
)
SELECT
    row_number() over (partition by 1 order by ship_state) as location_id,
    ship_city,
    ship_state,
    ship_country,
    ship_postal_code

from raw_locations
