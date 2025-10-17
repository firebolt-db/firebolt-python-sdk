"""Integration tests for transaction support in async environment."""


from pytest import raises

from firebolt.async_db import Connection, NotSupportedError
from firebolt.common.base_connection import (
    TRANSACTION_ID_PARAMETER,
    TRANSACTION_SEQUENCE_ID_PARAMETER,
)
from firebolt.utils.exception import ConnectionClosedError


async def safe_cleanup_table(connection: Connection, table_name: str) -> None:
    """Helper function to safely cleanup tables, ignoring any errors."""

    cleanup_cursor = connection.cursor()
    try:
        await cleanup_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    except Exception:
        pass  # Ignore cleanup errors


async def test_autocommit_default_state(connection: Connection) -> None:
    """Test that connections default to autocommit mode."""
    assert connection.autocommit is True
    assert connection.in_transaction is False


async def test_autocommit_property_transitions(connection: Connection) -> None:
    """Test autocommit property getter and setter."""
    # Test setting to False
    connection.autocommit = False
    assert connection.autocommit is False
    assert (
        connection.in_transaction is False
    )  # Transaction deferred until first statement

    # Test setting back to True
    connection.autocommit = True
    assert connection.autocommit is True
    assert connection.in_transaction is False


async def test_autocommit_with_active_transaction_commits(
    connection: Connection,
) -> None:
    """Test that setting autocommit=True commits any active transaction."""
    cursor = connection.cursor()

    # Start in non-autocommit mode
    connection.autocommit = False

    # Execute a statement that should start a transaction
    await cursor.execute("SELECT 1")

    # If server started a transaction, we should be in transaction state

    # Setting autocommit=True should commit any active transaction
    connection.autocommit = True
    assert connection.autocommit is True
    # After autocommit=True, any transaction should be committed
    assert connection.in_transaction is False


async def test_explicit_transaction_begin_commit(connection: Connection) -> None:
    """Test explicit transaction control with BEGIN TRANSACTION and COMMIT."""
    cursor = connection.cursor()

    # Start with autocommit off
    connection.autocommit = False

    # Explicitly begin a transaction
    await cursor.execute("BEGIN TRANSACTION")

    # Execute some statements within the transaction
    await cursor.execute("SELECT 1")
    await cursor.execute("SELECT 2")

    # Commit the transaction
    await connection.acommit()

    # Should no longer be in transaction
    assert connection.in_transaction is False


async def test_explicit_transaction_begin_rollback(connection: Connection) -> None:
    """Test explicit transaction control with BEGIN TRANSACTION and ROLLBACK."""
    cursor = connection.cursor()

    # Start with autocommit off
    connection.autocommit = False

    # Explicitly begin a transaction
    await cursor.execute("BEGIN TRANSACTION")

    # Execute some statements within the transaction
    await cursor.execute("SELECT 1")
    await cursor.execute("SELECT 2")

    # Rollback the transaction
    await connection.arollback()

    # Should no longer be in transaction
    assert connection.in_transaction is False


async def test_sync_transaction_methods_not_supported(connection: Connection) -> None:
    """Test that sync commit/rollback methods raise NotSupportedError for async connections."""
    with raises(NotSupportedError, match="Use acommit\\(\\) for async connections"):
        connection.commit()

    with raises(NotSupportedError, match="Use arollback\\(\\) for async connections"):
        connection.rollback()


async def test_transaction_methods_on_closed_connection(connection: Connection) -> None:
    """Test that transaction methods on closed connection raise ConnectionClosedError."""
    await connection.aclose()

    with raises(ConnectionClosedError, match="Unable to commit: Connection closed"):
        await connection.acommit()

    with raises(ConnectionClosedError, match="Unable to rollback: Connection closed"):
        await connection.arollback()

    with raises(
        ConnectionClosedError, match="Unable to set autocommit: Connection closed"
    ):
        connection.autocommit = False


async def test_multiple_cursors_share_transaction_state(connection: Connection) -> None:
    """Test that multiple cursors from the same connection share transaction state."""
    cursor1 = connection.cursor()
    cursor2 = connection.cursor()

    # Initially no transaction
    assert cursor1._in_transaction is False
    assert cursor2._in_transaction is False

    # Start with autocommit off
    connection.autocommit = False

    # Begin a transaction using cursor1
    await cursor1.execute("BEGIN TRANSACTION")

    # Both cursors should reflect the transaction state
    # This test verifies the synchronization mechanism works
    assert cursor1._in_transaction is True
    assert cursor2._in_transaction is True

    # Execute with cursor2 - should participate in same transaction
    await cursor2.execute("SELECT 1")

    # Commit using connection method
    if connection.in_transaction:
        await connection.acommit()

    # Both cursors should reflect the committed state
    assert cursor1._in_transaction is False
    assert cursor2._in_transaction is False


async def test_autocommit_mode_isolation(connection: Connection) -> None:
    """Test that in autocommit mode, each statement is isolated."""
    connection.autocommit = True

    cursor = connection.cursor()

    # Each statement should be automatically committed
    await cursor.execute("SELECT 1")
    assert connection.in_transaction is False

    await cursor.execute("SELECT 2")
    assert connection.in_transaction is False

    await cursor.execute("SELECT 3")
    assert connection.in_transaction is False


async def test_non_autocommit_mode_transaction_persistence(
    connection: Connection,
) -> None:
    """Test that in non-autocommit mode, transaction persists across statements."""
    connection.autocommit = False

    cursor = connection.cursor()

    # Begin transaction explicitly
    await cursor.execute("BEGIN TRANSACTION")

    # Multiple statements should be part of the same transaction
    await cursor.execute("SELECT 1")
    await cursor.execute("SELECT 2")

    # Commit the transaction
    if connection.in_transaction:
        await connection.acommit()
        assert connection.in_transaction is False


async def test_transaction_parameter_synchronization(connection: Connection) -> None:
    """Test that transaction_id parameters are synchronized across cursors."""
    cursor1 = connection.cursor()
    cursor2 = connection.cursor()

    connection.autocommit = False

    # When cursor1 receives transaction_id, all cursors should be updated
    await cursor1.execute("BEGIN TRANSACTION")

    # Both cursors should have the same set parameters
    # (assuming server sends transaction_id parameter)
    if hasattr(cursor1, "_set_parameters") and cursor1._set_parameters:
        if TRANSACTION_ID_PARAMETER in cursor1._set_parameters:
            assert cursor1._set_parameters.get(
                TRANSACTION_ID_PARAMETER
            ) == cursor2._set_parameters.get(TRANSACTION_ID_PARAMETER)

    # Commit should remove transaction_id from all cursors
    if connection.in_transaction:
        await connection.acommit()


async def test_transaction_basic_flow(connection: Connection) -> None:
    """Test basic transaction flow with real database operations."""
    table_name = "test_async_transaction_table"
    cursor = connection.cursor()
    try:
        # Set non-autocommit mode
        connection.autocommit = False

        # Begin transaction
        await cursor.execute("BEGIN TRANSACTION")

        # Create a temporary table within transaction
        await cursor.execute(
            f"CREATE TABLE {table_name} AS SELECT 1 as id, 'test' as name"
        )

        # Verify table exists
        await cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        result = await cursor.fetchone()
        assert result[0] == 1

        # Rollback the transaction
        await connection.arollback()

        # Table should no longer exist (rollback successful)
        with raises(
            Exception
        ):  # Should raise some kind of error for non-existent table
            await cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    finally:
        # Safe cleanup in case rollback failed
        await safe_cleanup_table(connection, table_name)


async def test_transaction_commit_persistence(connection: Connection) -> None:
    """Test that committed transactions persist in the database."""
    table_name = "test_async_commit_table"
    cursor = connection.cursor()
    try:
        # Set non-autocommit mode
        connection.autocommit = False

        # Begin transaction
        await cursor.execute("BEGIN TRANSACTION")

        # Create a temporary table within transaction
        await cursor.execute(
            f"CREATE TABLE {table_name} AS SELECT 1 as id, 'committed' as name"
        )

        # Commit the transaction
        await connection.acommit()

        # Table should still exist after commit
        await cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        result = await cursor.fetchone()
        assert result[0] == 1
    finally:
        # Safe cleanup
        await safe_cleanup_table(connection, table_name)


async def test_autocommit_behavior_with_ddl(connection: Connection) -> None:
    """Test autocommit behavior with DDL operations."""
    table_name = "test_async_autocommit_table"
    cursor = connection.cursor()
    try:
        # In autocommit mode, each statement should be immediately committed
        connection.autocommit = True

        await cursor.execute(
            f"CREATE TABLE {table_name} AS SELECT 1 as id, 'autocommit' as name"
        )

        # Statement should be immediately committed, table should exist
        await cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        result = await cursor.fetchone()
        assert result[0] == 1
    finally:
        # Safe cleanup
        await safe_cleanup_table(connection, table_name)


async def test_multiple_sequential_transactions(connection: Connection) -> None:
    """Test multiple sequential transactions in the database."""
    table1_name = "test_async_tx1"
    table2_name = "test_async_tx2"
    cursor = connection.cursor()
    try:
        connection.autocommit = False

        # First transaction
        await cursor.execute("BEGIN TRANSACTION")
        await cursor.execute(
            f"CREATE TABLE IF NOT EXISTS {table1_name} AS SELECT 1 as id"
        )
        await connection.acommit()

        # Second transaction
        await cursor.execute("BEGIN TRANSACTION")
        await cursor.execute(
            f"CREATE TABLE IF NOT EXISTS {table2_name} AS SELECT 2 as id"
        )
        await connection.acommit()

        # Both tables should exist
        await cursor.execute(f"SELECT COUNT(*) FROM {table1_name}")
        result = await cursor.fetchone()
        assert result[0] == 1

        await cursor.execute(f"SELECT COUNT(*) FROM {table2_name}")
        result = await cursor.fetchone()
        assert result[0] == 1
    finally:
        # Safe cleanup for both tables
        await safe_cleanup_table(connection, table1_name)
        await safe_cleanup_table(connection, table2_name)


async def test_transaction_with_streaming(connection: Connection) -> None:
    """Test transactions work with streaming queries."""
    cursor = connection.cursor()

    connection.autocommit = False

    # Begin transaction
    await cursor.execute("BEGIN TRANSACTION")

    # Execute streaming query within transaction
    await cursor.execute_stream("SELECT * FROM GENERATE_SERIES(1, 100)")

    # Fetch streaming results
    results = []
    async for row in cursor:
        results.append(row)
        if len(results) >= 10:  # Just fetch first 10 rows
            break

    assert len(results) == 10

    # Commit transaction
    if connection.in_transaction:
        await connection.acommit()


async def test_multi_cursor_transaction_parameter_sharing(
    connection: Connection,
) -> None:
    """Test transaction_id and transaction_sequence_id sharing across multiple cursors."""
    connection.autocommit = False

    # Create first cursor
    cursor1 = connection.cursor()

    # Begin a transaction with cursor1
    await cursor1.execute("BEGIN TRANSACTION")

    # Store initial transaction parameters if any were set
    initial_transaction_id = connection.transaction_id
    initial_sequence_id = connection.transaction_sequence_id

    # Create second cursor after transaction has started
    cursor2 = connection.cursor()

    # Both cursors should share the same transaction parameters
    if initial_transaction_id:
        assert (
            cursor1._set_parameters.get(TRANSACTION_ID_PARAMETER)
            == initial_transaction_id
        )
        assert (
            cursor2._set_parameters.get(TRANSACTION_ID_PARAMETER)
            == initial_transaction_id
        )

    if initial_sequence_id:
        assert (
            cursor1._set_parameters.get(TRANSACTION_SEQUENCE_ID_PARAMETER)
            == initial_sequence_id
        )
        assert (
            cursor2._set_parameters.get(TRANSACTION_SEQUENCE_ID_PARAMETER)
            == initial_sequence_id
        )

    # Execute queries with different cursors within the same transaction
    await cursor1.execute("SELECT 1 as first_cursor")
    result1 = await cursor1.fetchone()
    assert result1[0] == 1

    await cursor2.execute("SELECT 2 as second_cursor")
    result2 = await cursor2.fetchone()
    assert result2[0] == 2

    # Both cursors should still share transaction state
    assert cursor1._in_transaction == cursor2._in_transaction

    # Transaction parameters should remain consistent
    assert cursor1._set_parameters.get(
        TRANSACTION_ID_PARAMETER
    ) == cursor2._set_parameters.get(TRANSACTION_ID_PARAMETER)
    assert cursor1._set_parameters.get(
        TRANSACTION_SEQUENCE_ID_PARAMETER
    ) == cursor2._set_parameters.get(TRANSACTION_SEQUENCE_ID_PARAMETER)

    # Commit using connection method
    if connection.in_transaction:
        await connection.acommit()

    # After commit, transaction parameters should be cleared from both cursors
    assert (
        TRANSACTION_ID_PARAMETER not in cursor1._set_parameters
        or cursor1._set_parameters[TRANSACTION_ID_PARAMETER] is None
    )
    assert (
        TRANSACTION_ID_PARAMETER not in cursor2._set_parameters
        or cursor2._set_parameters[TRANSACTION_ID_PARAMETER] is None
    )
    assert (
        TRANSACTION_SEQUENCE_ID_PARAMETER not in cursor1._set_parameters
        or cursor1._set_parameters[TRANSACTION_SEQUENCE_ID_PARAMETER] is None
    )
    assert (
        TRANSACTION_SEQUENCE_ID_PARAMETER not in cursor2._set_parameters
        or cursor2._set_parameters[TRANSACTION_SEQUENCE_ID_PARAMETER] is None
    )

    # Both cursors should no longer be in transaction
    assert cursor1._in_transaction is False
    assert cursor2._in_transaction is False
