from db_utils import connect

params = {
'host': 'localhost',
'port': 5432,
'user': 'postgres',
'database': 'postgres',
}

query = """select 
    table_name,
    table_schema as schema_name, 
    table_catalog as database_name 
from information_schema.tables limit 5;"""

conn = connect(params)
cursor = conn.cursor()
cursor.execute(query)
rows = cursor.fetchall()

print("Got Columns: ", cursor.columns) 
print("Got Rows: ", rows)

cursor.close()
