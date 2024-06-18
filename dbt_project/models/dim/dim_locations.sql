{{
    config(
        materialized = 'table'
        )
}}

with raw_locations as (
    select distinct ship_city, ship_state, ship_country, ship_postal_code from {{ ref('cleansed_sales')}}
),
location as (SELECT
    row_number() over (order by ship_state, ship_postal_code, ship_country) as location_id,
    ship_city as city,
    ship_state as state,
    ship_country as country,
    ship_postal_code as postal_code,
    NULL as updated_city
from raw_locations)
select
    l.location_id,
    city,
    state,
    country,
    postal_code,
    Upper(cmo.orig_city) as updated_city
from location l
JOIN {{ref('city_name_optimizer')}} cmo on l.location_id = cmo.location_id
