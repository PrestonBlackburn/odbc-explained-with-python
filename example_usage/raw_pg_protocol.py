from db_utils import (
    ConnectionHandle, 
    startup, 
    execute, 
    process_chunk, 
    disconnect
)

parameters = {
    'host': 'localhost',
    'port': 5432,
    'user': 'postgres',
    'database': 'postgres',
}

handle = ConnectionHandle()
handle = startup(parameters, handle)

execute(handle, """select 
    table_name,
    table_schema as schema_name, 
    table_catalog as database_name 
from information_schema.tables limit 5;""")

columns, rows = process_chunk(handle)

print("Columns: ", columns)
print("Rows: ", rows)

disconnect(handle)

