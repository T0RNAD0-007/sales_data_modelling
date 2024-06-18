{{
    config(
        materialized = 'table'
        )
}}

with raw_promotions as (
    select distinct promotional_code from {{ ref('cleansed_sales')}}
)
SELECT
    row_number() over (order by promotional_code) as promotional_id,
    promotional_code
from raw_promotions
