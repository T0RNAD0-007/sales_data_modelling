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
    columns = ['INDEX', 'ORDER_ID', 'DATE', 'STATUS', 'FULFILMENT', 'SALES_CHANNEL', 'SHIP_SERVICE_LEVEL', 'STYLE', 'SKU', 'CATEGORY', 'SIZE', 'ASIN', 'COURIER_STATUS', 'QTY', 'CURRENCY', 'AMOUNT', 'SHIP_CITY', 'SHIP_STATE', 'SHIP_POSTAL_CODE', 'SHIP_COUNTRY', 'PROMOTION_IDS', 'B2B', 'FULFILLED_BY','UNKNOWN_FLAG']
    profile_columns=['column_name', 'column_count', 'table_count', 'min_length', 'max_length', 'distinct_count', 'fill_rate', 'null_rate']
    profile_initial = []

    for col in columns:
        result = read_sql_query(f"""select '{col}' as column_name, count(trim({col})) as column_count, count(*) as table_count,
         min(length(trim({col}))) as min_length, max(length(trim({col}))) as max_length,
         count(distinct trim({col})) as distinct_count, CAST((count(case when trim({col}) IS NOT NULL THEN 1 END)*100)/count(*) as NUMBER(5,2)) as fill_rate,
         CAST((count(case when trim({col}) IS NULL THEN 1 END)*100)/count(*) as NUMBER(5,2)) as null_rate from {table_name}""")
        profile_initial.append(result[0])

    profile_df = pd.DataFrame(data = profile_initial, columns = profile_columns)

    profile_df.to_excel('data_profiling.xlsx')
finally:
    curs.close()

conn.close()