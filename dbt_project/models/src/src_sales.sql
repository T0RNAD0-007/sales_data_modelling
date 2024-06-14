with raw_sales as (
    select * from AMAZON.RAW.RAW_SALES
),
raw_state_abbrevation as (
    select * from AMAZON.RAW.STATE_ABBREVATION
)
SELECT
    order_id,
    try_to_Date(date,'MM-DD-YY') as date,
    status as order_status,
    fulfilment as order_fulfilment,
    sales_channel,
    ship_service_level,
    sku as product_id,
    asin,
    courier_status,
    TRY_TO_NUMBER(qty,5,0) as order_qty,
    TRY_TO_NUMBER(currency,5,0) as currency,
    amount,
    upper(TRIM(ship_city)) as ship_city,
    CASE WHEN upper(rsa.state) is not null THEN upper(TRIM(rsa.state)) else upper(split_part(TRIM(rs.ship_state),'/',0)) end as ship_state,
    ship_postal_code,
    upper(TRIM(ship_country)) as ship_country,
    promotion_ids,
    b2b
from raw_sales rs left join raw_state_abbrevation rsa on TRIM(rs.ship_state) = TRIM(rsa.abbrevation)
