{{
    config(
        materialized = 'table',
        on_schema_change = 'fail',
        )
}}

with raw_sales as (
    select * from {{ ref('raw_sales')}}
),
raw_state_abbrevation as (
    select * from {{ source('DEV_STATIC','state_lookup')}}
),
city_lookup as (
    select * from {{ source('DEV_STATIC','city_names')}}
),
dedup_records as ( SELECT distinct
        order_id,
        try_to_Date(date,'MM-DD-YY') as date,
        status as order_status,
        fulfilment,
        sales_channel,
        ship_service_level,
        style as product_style,
        sku as product_sku,
        category as product_category,
        size as product_size,
        asin as asin_code,
        courier_status,
        TRY_TO_NUMBER(qty,5,0) as sales_quantity,
        'INR' as currency,
        CASE WHEN amount is NULL THEN 0.00 ELSE TRY_TO_NUMBER(amount,8,2) END as sales_amount,
        TRIM(regexp_replace(upper(ship_city), '[^a-zA-Z]+', ' ')) AS SHIP_CITY,
        CASE WHEN upper(rsa.state) is not null THEN upper(TRIM(rsa.state)) else upper(split_part(TRIM(rs.ship_state),'/',0)) end as ship_state,
        TRY_TO_NUMBER(TRIM(ship_postal_code),6,0) as ship_postal_code,
        upper(TRIM(ship_country)) as ship_country,
        promotion_ids as promotional_code,
        CASE WHEN TRIM(promotion_ids) is not null THEN TRY_TO_BOOLEAN(TRUE) else TRY_TO_BOOLEAN(FALSE) END as promotion_applied,
        TRY_TO_BOOLEAN(b2b) as b2b,
        CASE WHEN fulfilled_by is NULL THEN 'Amazon' else fulfilled_by end as order_fulfilled_by,
        case when sales_amount <> 0 and sales_quantity <> 0 and promotion_applied = FALSE then round(sales_amount/sales_quantity,2) else null end as unit_price,
        unknown_flag
    from raw_sales rs left join raw_state_abbrevation rsa on TRIM(rs.ship_state) = TRIM(rsa.abbrevation)
)
select row_number() over (order by 1) as sales_transaction_id, * from dedup_records

