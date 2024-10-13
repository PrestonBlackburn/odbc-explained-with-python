import db_utils.odbc_driver as odbc
from typing import List

def connect(params:dict):
    return Connection(params)

class Cursor:
    def __init__(self, handle: odbc.ConnectionHandle) -> None:
        self.rowcount = None
        self.description = None
        self.handle = handle
        self.columns = []
        
    def close(self) -> None:
        odbc.SQLDisconnect(self.handle)
        return
    
    def execute(self, query:str) -> None:
        odbc.SQLExecDirect(self.handle, query, len(query))
        self.results_generator = odbc.SQLFetch(self.handle)
        return
    
    def fetchone(self) -> tuple:
        # Not implemented
        pass
    
    def fetchall(self) -> List[tuple]:
        columns, rows = odbc.SQLGetData(self.results_generator)
        if columns != []:
            self.columns = columns
        return rows


class Connection:
    def __init__(self, params:dict):
        self.params = params
        self.host = params['host']
        self.handle = odbc.ConnectionHandle()

        odbc.SQLConnect(
            self.handle, 
            params['host'],
            len(params['host']),
            params['user'],
            len(params['user']),
            'none',
            4
        )

    def close(self) -> None:
        odbc.SQLDisconnect(self.handle)
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

    cursor.close()

