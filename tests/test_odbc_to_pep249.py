from db_utils.pep_249_odbc_manager import connect
import pytest


@pytest.fixture(scope='module', autouse=True)
def get_cursor(start_postgres, wait_for_postgres):

    parameters = {
        'host': 'localhost',
        'port': 5432,
        'user': 'postgres',
        'database': 'postgres',
    }

    conn = connect(parameters)
    cursor = conn.cursor()

    yield cursor
    cursor.close()

@pytest.fixture(autouse=True)
def test_query_execution(get_cursor):

    query = """select 
        table_name,
        table_schema as schema_name, 
        table_catalog as database_name 
    from information_schema.tables limit 5;"""
    get_cursor.execute(query)

    return get_cursor


def test_get_results(test_query_execution):

    rows = test_query_execution.fetchall()
    assert len(rows) == 5

