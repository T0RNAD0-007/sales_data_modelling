{{
    config(
        enabled = false
        )
}}

UPDATE {{source('DEV_DIM', 'dim_locations')}} DIM
SET DIM.UPDATED_CITY = OC.ORIG_CITY
FROM {{source('DEV_DIM', 'dim_city_optimizer')}} OC
WHERE DIM.CITY = OC.DIM_CITY