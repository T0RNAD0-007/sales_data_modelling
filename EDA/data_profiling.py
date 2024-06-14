import pandas as pd

from connection import create_conn

conn = create_conn()
curs = conn.cursor()

def read_sql_query(query):
    curs.execute(query)
    result = curs.fetchall()
    return result

table_name = 'sales'

try:
    columns = ['SHIP_POSTAL_CODE', 'SHIP_COUNTRY', 'SIZE', 'INDEX', 'FULFILMENT', 'AMOUNT', 'SKU', 'STATUS', 'PROMOTION_IDS', 'ORDER_ID', 'ASIN', 'SHIP_STATE', 'STYLE', 'CURRENCY', 'CATEGORY', 'FULFILLED_BY', 'UNKNOWN_FLAG', 'DATE', 'SALES_CHANNEL', 'SHIP_SERVICE_LEVEL', 'QTY', 'SHIP_CITY', 'B2B', 'COURIER_STATUS']

    profile_columns=['column_name', 'column_count', 'table_count', 'min_length', 'max_length', 'distinct_count', 'fill_rate', 'null_rate']
    profile_initial = []

    for col in columns:
        result = read_sql_query(f"""select '{col}' as column_name, count({col}) as column_count, count(*) as table_count,
         min(length({col})) as min_length, max(length({col})) as max_length,
         count(distinct {col}) as distinct_count, (count(case when {col} IS NOT NULL THEN 1 END)*100)/count(*) as fill_rate,
         (count(case when {col} IS NULL THEN 1 END)*100)/count(*) as null_rate from {table_name}""")
        profile_initial.append(result[0])

    profile_df = pd.DataFrame(data = profile_initial, columns = profile_columns)

    profile_df.to_excel
finally:
    curs.close()

conn.close()