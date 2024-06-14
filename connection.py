import snowflake.connector as sc

def create_conn():
    conn = sc.connect(
        user = 'xxx',
        password = 'xxx',
        account = 'xxx',
        database = 'RAW_SALES',
        warehouse = 'COMPUTE_WH',
        schema = 'PUBLIC'
    )

    return conn

