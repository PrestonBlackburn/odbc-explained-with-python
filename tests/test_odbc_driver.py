from db_utils import (
    ConnectionHandle, 
    SQLConnect, 
    SQLExecDirect, 
    SQLFetch, 
    SQLGetData, 
    SQLDisconnect
)
import logging
logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)


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

# this doesn't comletely align with ODBC as I use a generator instead of just passing the same handle
cursor = SQLFetch(handle)
columns, rows = SQLGetData(cursor)

SQLDisconnect(handle)

print("Got Columns: ", columns) 
print("Got Rows: ", rows)
