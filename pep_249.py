cursor = conn.cursor()
cursor.execute(SQL_QUERY)
records = cursor.fetchall()
for r in records:
    print(r)

# record object: prob comes from DataRow
# @dataclass
# class Row:
#     table_name:str
#     table_schema:str
#     table_catalog:str


def connect():
    return



def cursor():
    return

