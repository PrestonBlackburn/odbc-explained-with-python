import db_utils.simple_pg_protocol as pg
from typing import List

def connect(params:dict):
    return Connection(params)

class Cursor:
    def __init__(self, handle: pg.ConnectionHandle) -> None:
        self.rowcount = None
        self.description = None
        self.handle = handle
        self.columns = []
        
    def close(self) -> None:
        pg.disconnect(self.handle)
        return
    
    def execute(self, query:str) -> None:
        pg.execute(self.handle, query)
        self.results_generator = pg.fetch_message(self.handle)
        return
    
    def fetchone(self) -> tuple:
        columns, row = pg.get_row(self.results_generator)
        if columns != []:
            self.columns = columns
        return row
    
    def fetchall(self) -> List[tuple]:
        columns, rows = pg.get_data(self.results_generator)
        if columns != []:
            self.columns = columns
        return rows


class Connection:
    def __init__(self, params:dict):
        self.params = params
        self.handle = pg.startup(params, pg.ConnectionHandle())

    def close(self) -> None:
        pg.disconnect(self.handle)
        return
    
    def commit() -> None:
        # we won't be using this method
        # assume "autocommit"
        return
    
    def cursor(self) -> Cursor:
        return Cursor(self.handle)
    

if __name__ == "__main__":

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

    cursor.execute(query)

    for row in range(3):
        row = cursor.fetchone()
        print("Got Row: ", row)

    cursor.close()

