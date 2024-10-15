import pytest
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



@pytest.fixture(scope='module', autouse=True)
def establish_connection(start_postgres, wait_for_postgres):
    handle = ConnectionHandle()
    SQLConnect(
        handle,
        'localhost',
        9,
        'postgres',
        8,
        'none',
        4
    )        

    yield handle
    SQLDisconnect(handle)

@pytest.fixture(autouse=True)
def test_query_execution(establish_connection):
    query = """select 
        table_name,
        table_schema as schema_name, 
        table_catalog as database_name 
    from information_schema.tables limit 5;"""

    SQLExecDirect(
        establish_connection,
        query,
        len(query)
    )

    return establish_connection

def test_get_results(establish_connection, test_query_execution):
    cursor = SQLFetch(test_query_execution)
    columns, rows = SQLGetData(cursor)

    assert len(rows) == 5
    assert len(columns) == 3

