from dataclasses import dataclass
import socket
from typing import Generator, Tuple, List, Union

from db_utils.simple_pg_protocol import (
    ConnectionHandle,
    startup,
    execute,
    fetch_message,
    get_data,
    disconnect,
)

from logging import getLogger
_logger = getLogger(__name__)
# C reference Code:
# https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/connecting-with-sqlconnect?view=sql-server-ver16

# Generic ODBC Flow:
# https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/basic-odbc-application-steps?view=sql-server-ver16


parameters = {
    'host': 'localhost',
    'port': 5432,
    'user': 'postgres',
    'database': 'postgres',
}


@dataclass
class ReturnCode:
    code_name: str
    value: str = None

    def __post_init__(self):
        if self.code_name == "SQL_SUCCESS":
            self.value = '00000'
        elif self.code_name == "SQL_ERROR":
            self.value = 'HY000'

def SQLConnect(
        sqlhdbc: ConnectionHandle,
        server_name: str, # ex: 'localhost'
        server_name_length: int, # ex: 9
        user_name: str, # ex: 'postgres'
        user_name_length: int, # ex: 8
        authentication: str, # ex: 'none'
        authentication_length: int # ex: 4
) -> ReturnCode:
    _logger.debug("Running SQLConnect ODBC Function")
    try:
        parameters = {
            'host': server_name,
            'port': 5432,
            'user': user_name,
            'database': 'postgres',
        }

        startup(parameters, sqlhdbc)
        return_code = ReturnCode("SQL_SUCCESS")
    except Exception as e:
        _logger.error(e)
        return_code = ReturnCode("SQL_ERROR")

    return return_code


def SQLExecDirect(
        sqlhstmt: ConnectionHandle,
        statement: str,
        statement_length: int
) -> ReturnCode:
    _logger.debug("Running SQLExecDirect ODBC Function")
    try:
        execute(sqlhstmt, statement)
        return_code = ReturnCode("SQL_SUCCESS")
    except Exception as e:
        _logger.error(e)
        return_code = ReturnCode("SQL_ERROR")
    
    return return_code


def SQLFetch(
    sqlhstmt: ConnectionHandle,
) -> Union[Generator[bytes, None, None], ReturnCode]:
    _logger.debug("Running SQLFetch ODBC Function")
    try:
        cursor = fetch_message(sqlhstmt)
        return_code = ReturnCode("SQL_SUCCESS")
    except Exception as e:
        _logger.error(e)
        return_code = ReturnCode("SQL_ERROR")
        return return_code
    
    return cursor


def SQLGetData(
        # this should technically take in a ConnectionHandle 
        cursor: Generator[bytes, None, None]
) -> Union[Tuple[list, List[tuple]], ReturnCode]:
    _logger.debug("Running SQLGetData ODBC Function")
    try:
        columns, rows = get_data(cursor)
        return_code = ReturnCode("SQL_SUCCESS")
    except Exception as e:
        _logger.error(e)
        return_code = ReturnCode("SQL_ERROR")
        return return_code
    
    return columns, rows


def SQLDisconnect(
        sqlhstmt: ConnectionHandle
) -> ReturnCode:
    _logger.debug("Running SQLDisconnect ODBC Function")
    try:
        disconnect(sqlhstmt)
        return_code = ReturnCode("SQL_SUCCESS")
    except Exception as e:
        _logger.error(e)
        return_code = ReturnCode("SQL_ERROR")
    
    return return_code


if __name__ == "__main__":
    
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





