{{
    config(
        materialized = 'view'
        )
}}

with state_similarity as (
    SELECT
        dl.city as dim_city,
        cn.city_name as orig_city,
        JAROWINKLER_SIMILARITY(UPPER(dl.city), UPPER(cn.city_name)) as num
    FROM (SELECT DISTINCT CITY FROM {{source('DEV_DIM', 'dim_locations')}}) dl
    CROSS JOIN (SELECT DISTINCT city_name from {{source('DEV_STATIC', 'city_names')}}) cn
    WHERE JAROWINKLER_SIMILARITY(upper(dl.city), UPPER(cn.city_name)) >= 85
),
state_similarity_high as (
    select
    dim_city,
    orig_city,
    num,
    row_number() over (partition by dim_city,orig_city order by num desc) as rn
    from state_similarity
)
select dim_city, orig_city, num from state_similarity_high where rn = 1
