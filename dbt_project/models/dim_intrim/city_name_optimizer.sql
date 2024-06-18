{{
    config(
        materialized = 'table',
        enabled = true
        )
}}

WITH raw_locations as (
    select distinct ship_city, ship_state, ship_country, ship_postal_code from {{ ref('cleansed_sales')}}
),
location as (SELECT
    row_number() over (order by ship_state, ship_postal_code, ship_country) as location_id,
    ship_city as city,
    ship_state as state
FROM raw_locations),
TOKENIZED_CITY_DIM as (SELECT
  distinct T.location_id,
  T.state,
  REPLACE(X.VALUE,'"') AS CITY_DIM
FROM (
  SELECT
    location_id,
    STATE,
    SPLIT(CITY,' ') AS UUID_ARR_CLEANED
    FROM (SELECT location_id,CITY, STATE FROM location)) T,
  LATERAL FLATTEN(T.UUID_ARR_CLEANED) X),
TOKENIZED_CITY_STATIC as (SELECT
  distinct T.city_id,
  T.state_name,
  REPLACE(X.VALUE,'"') AS CITY_STATIC
FROM (
  SELECT
    city_id,
    state_name,
    SPLIT(CITY_name,' ') AS UUID_ARR_CLEANED
    FROM (SELECT city_id,regexp_replace(CITY_name,'[^a-zA-Z]',' ') as city_name,state_name FROM {{ source('DEV_STATIC','city_names')}})) T,
  LATERAL FLATTEN(T.UUID_ARR_CLEANED) X),
CITY_SIMILARITY_INDEX as (
SELECT TC.location_id,
    CN.CITY_ID,
    state_name,
    TRIM(TC.city_dim) as city_dim,
    TRIM(cn.CITY_STATIC) as CITY_STATIC,
    JAROWINKLER_SIMILARITY(TRIM(UPPER(CN.CITY_STATIC)), TRIM(TC.CITY_DIM)) as similirity_index
from TOKENIZED_CITY_DIM TC
cross JOIN TOKENIZED_CITY_STATIC CN
WHERE UPPER(TRIM(CN.STATE_NAME)) = UPPER(TC.STATE)
    AND LENGTH(TRIM(CITY_DIM)) > 2
    and LENGTH(TRIM(CITY_STATIC)) > 2
    and similirity_index > 89 order by similirity_index asc),
final as (
SELECT location_id,
    city_id,
    city_dim,
    city_static,
    similirity_index,
    row_number() over (partition by location_id, city_dim order by similirity_index desc) as rank_city
from CITY_SIMILARITY_INDEX),
finall as (
SELECT location_id,
    city_id,
    city_dim,
    city_static,
    similirity_index,
    row_number() over (partition by location_id order by city_dim,city_static desc) as random_rank_city
FROM final where rank_city = 1
)
select location_id,city_dim, city_static, cn.city_name as orig_city from finall
JOIN {{ source('DEV_STATIC','city_names')}} cn where cn.city_id = finall.city_id and random_rank_city = 1
