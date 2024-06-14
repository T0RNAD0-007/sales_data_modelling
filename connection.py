import snowflake.connector as sc

def create_conn():
    conn = sc.connect(
        user = 'shivambhongade',
        password = 'S#!v@m@1234',
        account = 'keuzwie-bc34931',
        database = 'RAW_SALES',
        warehouse = 'COMPUTE_WH',
        schema = 'PUBLIC'
    )

    return conn

