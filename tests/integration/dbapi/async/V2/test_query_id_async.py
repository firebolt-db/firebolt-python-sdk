import time
from random import randint
from typing import Callable, List

import trio
from pytest import mark, raises

from firebolt.async_db import Connection
from firebolt.utils.exception import FireboltError

QUERY = "SELECT 42 AS query_id_test"
FAILING_QUERY = "SELECT CAST('query_id_test' AS INT)"
# Query history is updated asynchronously
HISTORY_TIMEOUT_SECONDS = 120


def _label(name: str) -> str:
    return f"{name}_{randint(100000, 999999)}"


async def _history_query_ids(
    connection: Connection, label: str, query_text: str, failed: bool = False
) -> List[str]:
    status = (
        "status NOT IN ('STARTED_EXECUTION', 'ENDED_SUCCESSFULLY')"
        if failed
        else "status = 'ENDED_SUCCESSFULLY'"
    )
    deadline = time.monotonic() + HISTORY_TIMEOUT_SECONDS
    async with connection.cursor() as c:
        while True:
            # The LIKE skips the labeled `select 1` that validates SET query_label
            await c.execute(
                "SELECT query_id FROM information_schema.engine_query_history "
                f"WHERE query_label = ? AND query_text LIKE ? AND {status}",
                [label, f"{query_text}%"],
            )
            rows = await c.fetchall()
            if rows or time.monotonic() > deadline:
                break
            await trio.sleep(5)
    assert rows, (
        f"no {'failed' if failed else 'finished'} query labeled {label!r} showed "
        f"up in engine_query_history within {HISTORY_TIMEOUT_SECONDS}s"
    )
    return [row[0] for row in rows]


async def test_query_id_execute(connection: Connection) -> None:
    """cursor.query_id is the id the engine records in query history."""
    label = _label("test_query_id_execute")
    async with connection.cursor() as c:
        assert c.query_id == ""
        await c.execute(f"SET query_label = '{label}'")
        await c.execute(QUERY)
        assert await c.fetchall() == [[42]]
        query_id = c.query_id
    assert query_id
    assert await _history_query_ids(connection, label, QUERY) == [query_id]


async def test_query_id_execute_stream(connection: Connection) -> None:
    """cursor.query_id is set for a streamed query."""
    label = _label("test_query_id_stream")
    async with connection.cursor() as c:
        await c.execute(f"SET query_label = '{label}'")
        await c.execute_stream(QUERY)
        assert await c.fetchall() == [[42]]
        query_id = c.query_id
    assert query_id
    assert await _history_query_ids(connection, label, QUERY) == [query_id]


async def test_query_id_per_statement(connection: Connection) -> None:
    """Each statement of a multi-statement query has its own query_id."""
    label = _label("test_query_id_per_statement")
    async with connection.cursor() as c:
        await c.execute(f"SET query_label = '{label}'")
        await c.execute(f"{QUERY}; {QUERY}")
        first = c.query_id
        assert await c.nextset() is True
        second = c.query_id
    assert first
    assert second
    assert first != second
    assert sorted(await _history_query_ids(connection, label, QUERY)) == sorted(
        [first, second]
    )


async def test_query_id_per_statement_stream(connection: Connection) -> None:
    """Each statement of a streamed multi-statement query has its own query_id."""
    async with connection.cursor() as c:
        await c.execute_stream("SELECT 1; SELECT 2")
        first = c.query_id
        assert await c.nextset() is True
        second = c.query_id
    assert first
    assert second
    assert first != second


async def test_query_id_changes_between_executes(connection: Connection) -> None:
    """Every execute on the same cursor reports the id of its own query."""
    async with connection.cursor() as c:
        await c.execute(QUERY)
        first = c.query_id
        await c.execute(QUERY)
        second = c.query_id
    assert first
    assert second
    assert first != second


async def test_query_id_set_statement(connection: Connection) -> None:
    """A SET is applied client-side, so it has no query_id of its own."""
    async with connection.cursor() as c:
        await c.execute(QUERY)
        assert c.query_id
        await c.execute("SET query_label = 'query_id_test'")
        assert c.query_id == ""


async def test_query_id_insert(
    connection: Connection, create_drop_test_table_setup_teardown_async: Callable
) -> None:
    """A statement without a result set still reports its query_id."""
    table = create_drop_test_table_setup_teardown_async
    label = _label("test_query_id_insert")
    insert = f"INSERT INTO \"{table}\" VALUES (1, 'a')"
    async with connection.cursor() as c:
        await c.execute(f"SET query_label = '{label}'")
        await c.execute(insert)
        query_id = c.query_id
    assert query_id
    assert await _history_query_ids(connection, label, insert) == [query_id]


async def test_query_id_failed_query(connection: Connection) -> None:
    """A failed query reports the id the engine recorded for it."""
    label = _label("test_query_id_failed")
    async with connection.cursor() as c:
        await c.execute(f"SET query_label = '{label}'")
        with raises(FireboltError):
            await c.execute(FAILING_QUERY)
        query_id = c.query_id
    assert query_id
    assert await _history_query_ids(connection, label, FAILING_QUERY, failed=True) == [
        query_id
    ]


async def test_query_id_failed_statement_of_several(connection: Connection) -> None:
    """When a later statement fails, query_id is not the id of an earlier one."""
    label = _label("test_query_id_failed_second")
    async with connection.cursor() as c:
        await c.execute(f"SET query_label = '{label}'")
        with raises(FireboltError):
            await c.execute(f"{QUERY}; {FAILING_QUERY}")
        query_id = c.query_id
    assert query_id
    assert query_id not in await _history_query_ids(connection, label, QUERY)
    assert await _history_query_ids(connection, label, FAILING_QUERY, failed=True) == [
        query_id
    ]


async def test_query_id_failed_statement_stream(connection: Connection) -> None:
    """A streamed statement that fails reports its own id."""
    label = _label("test_query_id_failed_stream")
    async with connection.cursor() as c:
        await c.execute(f"SET query_label = '{label}'")
        # The error surfaces from execute_stream or from nextset, depending on
        # whether the engine reports it in the HTTP status or in the stream
        with raises(FireboltError):
            await c.execute_stream(f"{QUERY}; {FAILING_QUERY}")
            while await c.nextset():
                pass
        query_id = c.query_id
    assert query_id
    assert await _history_query_ids(connection, label, FAILING_QUERY, failed=True) == [
        query_id
    ]


# Firebolt Core has no server-side async execution
@mark.parametrize("connection_factory", ["remote"], indirect=True)
async def test_query_id_execute_async(
    connection: Connection, create_drop_test_table_setup_teardown_async: Callable
) -> None:
    """A server-side async query has no query_id, only a token."""
    table = create_drop_test_table_setup_teardown_async
    async with connection.cursor() as c:
        await c.execute(QUERY)
        assert c.query_id
        await c.execute_async(f"INSERT INTO \"{table}\" VALUES (1, 'a')")
        assert c.async_query_token
        assert c.query_id == ""
