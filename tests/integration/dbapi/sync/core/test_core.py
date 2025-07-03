from pytest import fixture

from firebolt.db import connect


@fixture(scope="function")
def connection(core_auth):
    """Create a connection to Firebolt Core for testing."""
    conn = connect(auth=core_auth, database="firebolt")
    yield conn
    conn.close()


@fixture
def cursor(connection):
    """Create a cursor for testing."""
    return connection.cursor()


# Connection Tests


def test_core_connection_basic(core_auth):
    """Test basic connection to Firebolt Core."""
    # Connect with default database (firebolt)
    with connect(auth=core_auth) as connection:
        assert connection is not None
        cursor = connection.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchall()
        # FireboltCore returns a different format than standard Firebolt
        assert result == [[1]] or result == [(1,)]


def test_core_connection_database(core_auth):
    """Test connection to Firebolt Core with specified database."""
    # Connect with specified database
    with connect(auth=core_auth, database="firebolt") as connection:
        assert connection is not None
        cursor = connection.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchall()
        # FireboltCore returns a different format than standard Firebolt
        assert result == [[1]] or result == [(1,)]


def test_core_api_endpoint_ignored(core_auth):
    """Test that api_endpoint is ignored with FireboltCore."""
    # Connect with custom api_endpoint (should be ignored)
    with connect(
        auth=core_auth, api_endpoint="https://ignored-endpoint.com"
    ) as connection:
        assert connection is not None
        cursor = connection.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchall()
        # FireboltCore returns a different format than standard Firebolt
        assert result == [[1]] or result == [(1,)]

        # The connection should use the FireboltCore URL, not the api_endpoint
        assert "ignored-endpoint.com" not in connection.engine_url


# Cursor Tests


def test_core_cursor_execute_basic(cursor):
    """Test basic query execution with Firebolt Core cursor."""
    cursor.execute("SELECT 1 as one, 2 as two")

    # Check description is set correctly
    assert len(cursor.description) == 2
    assert cursor.description[0][0] == "one"
    assert cursor.description[1][0] == "two"

    # Check result is fetched correctly
    result = cursor.fetchall()
    assert len(result) == 1

    # FireboltCore can return either list or tuple format
    row = result[0]
    assert row[0] == 1
    assert row[1] == 2


def test_core_cursor_execute_complex_types(cursor):
    """Test handling of complex data types with Firebolt Core cursor."""
    cursor.execute(
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
    row = cursor.fetchone()
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


def test_core_cursor_parameter_filtering(core_auth):
    """Test that problematic parameters are filtered in cursor execution."""
    # Connect with additional parameters that should be filtered out
    connection = connect(
        auth=core_auth,
        database="firebolt",
        additional_parameters={
            # These should be filtered out and not cause errors
            "protocol": "https",
            "host": "example.com",
            "port": "9999",
            "connection_type": "wrong_type",
        },
    )

    try:
        cursor = connection.cursor()

        # Execute a query that would fail if parameters weren't filtered
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert result[0] == 1

        # Check that we can still run queries with additional parameters in the connection
        # These parameters would have been filtered out already

        # Run a query - parameters are already filtered during connection
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert result[0] == 1
    finally:
        connection.close()


def test_core_cursor_error_handling(cursor):
    """Test error handling in Firebolt Core cursor."""
    from firebolt.utils.exception import FireboltStructuredError

    # Syntax error
    try:
        cursor.execute("SELEC 1")  # Intentional typo
        assert False, "Should have raised an exception"
    except FireboltStructuredError as e:
        # FireboltCore may raise FireboltStructuredError directly
        assert (
            "SELEC" in str(e) or "syntax" in str(e).lower()
        ), "Error should mention the syntax issue"

    # Table doesn't exist error
    try:
        cursor.execute("SELECT * FROM nonexistent_table")
        assert False, "Should have raised an exception"
    except FireboltStructuredError as e:
        # FireboltCore may raise FireboltStructuredError directly
        assert "nonexistent_table" in str(e) or "not exist" in str(
            e
        ), "Error should mention the table"


def test_core_cursor_multi_statement(cursor):
    """Test multi-statement execution in Firebolt Core cursor."""
    # Drop the table first to ensure clean state
    cursor.execute("DROP TABLE IF EXISTS temp_test_table")

    try:
        # Create a temporary table, insert data, and query it in one statement
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS temp_test_table (id INT, value TEXT);
            TRUNCATE TABLE temp_test_table;
            INSERT INTO temp_test_table VALUES (1, 'one'), (2, 'two');
            SELECT * FROM temp_test_table ORDER BY id;
            """
        )
        cursor.nextset()
        cursor.nextset()
        cursor.nextset()  # Move to the 4th result set (the SELECT statement
        # Results should be from the SELECT statement
        results = cursor.fetchall()
        assert len(results) == 2

    finally:
        # Always clean up, even if test fails
        cursor.execute("DROP TABLE IF EXISTS temp_test_table")
    row1 = results[0]
    row2 = results[1]

    assert row1[0] == 1
    assert row1[1] == "one"
    assert row2[0] == 2
    assert row2[1] == "two"


def test_core_cursor_fetchmany(cursor):
    """Test fetchmany functionality in Firebolt Core cursor."""
    # Create a table with some data manually
    cursor.execute("DROP TABLE IF EXISTS test_fetch_table")
    cursor.execute("CREATE TABLE IF NOT EXISTS test_fetch_table (id INT)")

    # Insert 10 rows manually
    insert_values = ", ".join([f"({i})" for i in range(10)])
    cursor.execute(f"INSERT INTO test_fetch_table VALUES {insert_values}")

    try:
        # Query all rows
        cursor.execute("SELECT * FROM test_fetch_table ORDER BY id")
        batch1 = cursor.fetchmany(3)
        assert len(batch1) == 3

        # Check values rather than exact positions since order might not be guaranteed
        values = [row[0] for row in batch1]
        assert set(values) == {0, 1, 2}

        # Check remaining batches
        batch2 = cursor.fetchmany(4)
        assert len(batch2) == 4
        values = [row[0] for row in batch2]
        assert set(values) == {3, 4, 5, 6}

        # Fetch remaining rows
        batch3 = cursor.fetchmany(10)  # More than remaining rows
        assert len(batch3) == 3
        values = [row[0] for row in batch3]
        assert set(values) == {7, 8, 9}

        # No more rows
        batch4 = cursor.fetchmany(1)
        assert len(batch4) == 0
    finally:
        cursor.execute("DROP TABLE IF EXISTS test_fetch_table")
