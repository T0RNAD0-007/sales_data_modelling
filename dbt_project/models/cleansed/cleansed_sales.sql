{{
    config(
        materialized = 'table',
        on_schema_change = 'fail',
        )
}}

with raw_sales as (
    select * from {{ source('DEV_RAW', 'sales')}}
),
raw_state_abbrevation as (
    select * from {{ source('DEV_STAGE', 'state_lookup')}}
),
dedup_records as ( SELECT distinct
        order_id,
        try_to_Date(date,'MM-DD-YY') as date,
        status as order_status,
        fulfilment,
        sales_channel,
        ship_service_level,
        sku as product_sku,
        category as product_category,
        size as product_size,
        asin as asin_code,
        courier_status,
        TRY_TO_NUMBER(qty,5,0) as sales_quantity,
        currency,
        TRY_TO_NUMBER(amount,8,0) as sales_amount,
        upper(TRIM(ship_city)) as ship_city,
        CASE WHEN upper(rsa.state) is not null THEN upper(TRIM(rsa.state)) else upper(split_part(TRIM(rs.ship_state),'/',0)) end as ship_state,
        TRY_TO_NUMBER(TRIM(ship_postal_code),6,0) as ship_postal_code,
        upper(TRIM(ship_country)) as ship_country,
        promotion_ids,
        TRY_TO_BOOLEAN(b2b) as b2b,
        fulfilled_by as order_fulfilled_by
    from raw_sales rs left join raw_state_abbrevation rsa on TRIM(rs.ship_state) = TRIM(rsa.abbrevation)
)
select row_number() over (order by 1) as sales_transaction_id, * from dedup_records

