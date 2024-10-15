import pytest
from db_utils import (
    ConnectionHandle,
    startup,
    execute,
    process_chunk,
    disconnect
)


@pytest.fixture(scope='module', autouse=True)
def establish_connection(start_postgres, wait_for_postgres):

    parameters = {
        'host': 'localhost',
        'port': 5432,
        'user': 'postgres',
        'database': 'postgres',
    }

    handle = ConnectionHandle()
    handle = startup(parameters, handle)

    yield handle
    disconnect(handle)

@pytest.fixture(autouse=True)
def test_query_execution(establish_connection):
    query = """select 
        table_name,
        table_schema as schema_name, 
        table_catalog as database_name 
    from information_schema.tables limit 5;"""

    execute(establish_connection, query)

    return establish_connection


def test_get_results(test_query_execution):

    columns, rows = process_chunk(test_query_execution)
    assert len(rows) == 5
    assert len(columns) == 3

