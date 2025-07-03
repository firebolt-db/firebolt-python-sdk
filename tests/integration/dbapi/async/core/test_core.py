"""Integration tests for asynchronous Firebolt Core functionality."""


from pytest import fixture

from firebolt.async_db import connect
from firebolt.utils.exception import FireboltStructuredError, ProgrammingError


@fixture
async def connection(core_auth):
    """Create a connection to Firebolt Core for testing."""
    conn = await connect(auth=core_auth, database="firebolt")
    yield conn
    await conn.aclose()


@fixture
def cursor(connection):
    """Create a cursor for testing."""
    return connection.cursor()


# Connection Tests


async def test_core_connection_basic(core_auth):
    """Test basic connection to Firebolt Core."""
    # Connect with default database (firebolt)
    async with await connect(auth=core_auth) as connection:
        assert connection is not None
        cursor = connection.cursor()
        await cursor.execute("SELECT 1")
        result = await cursor.fetchall()
        # Support both list and tuple formats that Firebolt Core might return
        assert result == [(1,)] or result == [[1]]


async def test_core_connection_database(core_auth):
    """Test connection to Firebolt Core with specified database."""
    # Connect with specified database
    async with await connect(auth=core_auth, database="firebolt") as connection:
        assert connection is not None
        cursor = connection.cursor()
        await cursor.execute("SELECT 1")
        result = await cursor.fetchall()
        # Support both list and tuple formats that Firebolt Core might return
        assert result == [(1,)] or result == [[1]]


async def test_core_api_endpoint_ignored(core_auth):
    """Test that api_endpoint is ignored with FireboltCore."""
    # Connect with custom api_endpoint (should be ignored)
    async with await connect(
        auth=core_auth, api_endpoint="https://ignored-endpoint.com"
    ) as connection:
        assert connection is not None
        cursor = connection.cursor()
        await cursor.execute("SELECT 1")
        result = await cursor.fetchall()
        # Support both list and tuple formats that Firebolt Core might return
        assert result == [(1,)] or result == [[1]]

        # The connection should use the FireboltCore URL, not the api_endpoint
        assert "ignored-endpoint.com" not in connection.engine_url


# Cursor Tests


async def test_core_cursor_execute_basic(cursor):
    """Test basic query execution with Firebolt Core cursor."""
    await cursor.execute("SELECT 1 as one, 2 as two")

    # Check description is set correctly
    assert len(cursor.description) == 2
    assert cursor.description[0][0] == "one"
    assert cursor.description[1][0] == "two"

    # Check result is fetched correctly
    result = await cursor.fetchall()
    assert len(result) == 1

    # FireboltCore can return either list or tuple format
    row = result[0]
    assert row[0] == 1
    assert row[1] == 2


async def test_core_cursor_execute_complex_types(cursor):
    """Test handling of complex data types with Firebolt Core cursor."""
    await cursor.execute(
        """
        SELECT 
            'text' as text_value,
            123.45 as float_value,
            ARRAY[1, 2, 3] as array_value,
            CAST('2023-01-01' AS DATE) as date_value
        """
    )

    # Check description is set correctly
    assert len(cursor.description) == 4
    assert cursor.description[0][0] == "text_value"
    assert cursor.description[1][0] == "float_value"
    assert cursor.description[2][0] == "array_value"
    assert cursor.description[3][0] == "date_value"

    # Check result is fetched correctly
    row = await cursor.fetchone()
    assert row[0] == "text"
    assert abs(row[1] - 123.45) < 0.001  # Float comparison with tolerance
    # FireboltCore may return arrays in different formats depending on version
    array_val = row[2]
    # Check it's either a list or tuple containing 1, 2, 3
    assert (
        isinstance(array_val, (list, tuple))
        and len(array_val) == 3
        and 1 in array_val
        and 2 in array_val
        and 3 in array_val
    )

    # Date should be returned as a string in ISO format
    assert "2023-01-01" in str(row[3])


async def test_core_cursor_error_handling(cursor):
    """Test error handling in Firebolt Core cursor."""
    # Syntax error
    try:
        await cursor.execute("SELEC 1")  # Intentional typo
        assert False, "Should have raised an exception"
    except ProgrammingError:
        # This is the expected exception
        pass

    # Table doesn't exist error
    try:
        await cursor.execute("SELECT * FROM nonexistent_table")
        assert False, "Should have raised an exception"
    except FireboltStructuredError:
        # This is the expected exception
        pass


async def test_core_cursor_multi_statement(cursor):
    """Test multi-statement execution in Firebolt Core cursor."""
    # Ensure clean state first
    await cursor.execute("DROP TABLE IF EXISTS temp_test_table")

    try:
        # Create a temporary table, insert data, and query it in one statement
        await cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS temp_test_table (id INT, value TEXT);
            TRUNCATE TABLE temp_test_table;
            INSERT INTO temp_test_table VALUES (1, 'one'), (2, 'two');
            SELECT * FROM temp_test_table ORDER BY id;
            """
        )

        # Move to the 4th result set (the SELECT statement)
        await cursor.nextset()
        await cursor.nextset()
        await cursor.nextset()
        # Results should be from the SELECT statement
        results = await cursor.fetchall()
        assert len(results) == 2

        # FireboltCore can return either list or tuple format
        row1 = results[0]
        row2 = results[1]
    finally:
        # Always clean up, even if test fails
        await cursor.execute("DROP TABLE IF EXISTS temp_test_table")

    assert row1[0] == 1
    assert row1[1] == "one"
    assert row2[0] == 2
    assert row2[1] == "two"


async def test_core_cursor_fetchmany(cursor):
    """Test fetchmany functionality in Firebolt Core cursor."""
    # Create a table with some data manually
    await cursor.execute("DROP TABLE IF EXISTS test_fetch_table")
    await cursor.execute("CREATE TABLE IF NOT EXISTS test_fetch_table (id INT)")

    # Insert 10 rows manually
    insert_values = ", ".join([f"({i})" for i in range(10)])
    await cursor.execute(f"INSERT INTO test_fetch_table VALUES {insert_values}")
    try:
        # Query all rows
        await cursor.execute("SELECT * FROM test_fetch_table ORDER BY id")

        # Fetch rows in batches
        batch1 = await cursor.fetchmany(3)
        assert len(batch1) == 3
        assert batch1[0][0] == 0
        assert batch1[2][0] == 2

        batch2 = await cursor.fetchmany(4)
        assert len(batch2) == 4
        assert batch2[0][0] == 3
        assert batch2[3][0] == 6

        # Fetch remaining rows
        batch3 = await cursor.fetchmany(10)  # More than remaining rows
        assert len(batch3) == 3
        assert batch3[0][0] == 7
        assert batch3[2][0] == 9

        # No more rows
        batch4 = await cursor.fetchmany(1)
        assert len(batch4) == 0
    finally:
        await cursor.execute("DROP TABLE IF EXISTS test_fetch_table")
