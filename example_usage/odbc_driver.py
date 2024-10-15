from db_utils import (
    ConnectionHandle, 
    SQLConnect, 
    SQLExecDirect, 
    SQLFetch, 
    SQLGetData, 
    SQLDisconnect
)


handle = ConnectionHandle()
query = """select 
    table_name,
    table_schema as schema_name, 
    table_catalog as database_name 
from information_schema.tables limit 5;"""

SQLConnect(
    handle,
    'localhost',
    9,
    'postgres',
    8,
    'none',
    4
)

SQLExecDirect(
    handle,
    query,
    len(query)
)

cursor = SQLFetch(handle)
columns, rows = SQLGetData(cursor)

SQLDisconnect(handle)

print("Got Columns: ", columns) 
print("Got Rows: ", rows)
