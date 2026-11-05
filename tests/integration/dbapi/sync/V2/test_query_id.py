import time
from random import randint
from typing import Callable, List

from pytest import mark, raises

from firebolt.db import Connection
from firebolt.utils.exception import FireboltError

QUERY = "SELECT 42 AS query_id_test"
FAILING_QUERY = "SELECT CAST('query_id_test' AS INT)"
# Query history is updated asynchronously
HISTORY_TIMEOUT_SECONDS = 120


def _label(name: str) -> str:
    return f"{name}_{randint(100000, 999999)}"


def _history_query_ids(
    connection: Connection, label: str, query_text: str, failed: bool = False
) -> List[str]:
    status = (
        "status NOT IN ('STARTED_EXECUTION', 'ENDED_SUCCESSFULLY')"
        if failed
        else "status = 'ENDED_SUCCESSFULLY'"
    )
    deadline = time.monotonic() + HISTORY_TIMEOUT_SECONDS
    with connection.cursor() as c:
        while True:
            # The LIKE skips the labeled `select 1` that validates SET query_label
            c.execute(
                "SELECT query_id FROM information_schema.engine_query_history "
                f"WHERE query_label = ? AND query_text LIKE ? AND {status}",
                [label, f"{query_text}%"],
            )
            rows = c.fetchall()
            if rows or time.monotonic() > deadline:
                break
            time.sleep(5)
    assert rows, (
        f"no {'failed' if failed else 'finished'} query labeled {label!r} showed "
        f"up in engine_query_history within {HISTORY_TIMEOUT_SECONDS}s"
    )
    return [row[0] for row in rows]


def test_query_id_execute(connection: Connection) -> None:
    """cursor.query_id is the id the engine records in query history."""
    label = _label("test_query_id_execute")
    with connection.cursor() as c:
        assert c.query_id == ""
        c.execute(f"SET query_label = '{label}'")
        c.execute(QUERY)
        assert c.fetchall() == [[42]]
        query_id = c.query_id
    assert query_id
    assert _history_query_ids(connection, label, QUERY) == [query_id]


def test_query_id_execute_stream(connection: Connection) -> None:
    """cursor.query_id is set for a streamed query."""
    label = _label("test_query_id_stream")
    with connection.cursor() as c:
        c.execute(f"SET query_label = '{label}'")
        c.execute_stream(QUERY)
        assert c.fetchall() == [[42]]
        query_id = c.query_id
    assert query_id
    assert _history_query_ids(connection, label, QUERY) == [query_id]


def test_query_id_per_statement(connection: Connection) -> None:
    """Each statement of a multi-statement query has its own query_id."""
    label = _label("test_query_id_per_statement")
    with connection.cursor() as c:
        c.execute(f"SET query_label = '{label}'")
        c.execute(f"{QUERY}; {QUERY}")
        first = c.query_id
        assert c.nextset() is True
        second = c.query_id
    assert first
    assert second
    assert first != second
    assert sorted(_history_query_ids(connection, label, QUERY)) == sorted(
        [first, second]
    )


def test_query_id_per_statement_stream(connection: Connection) -> None:
    """Each statement of a streamed multi-statement query has its own query_id."""
    with connection.cursor() as c:
        c.execute_stream("SELECT 1; SELECT 2")
        first = c.query_id
        assert c.nextset() is True
        second = c.query_id
    assert first
    assert second
    assert first != second


def test_query_id_changes_between_executes(connection: Connection) -> None:
    """Every execute on the same cursor reports the id of its own query."""
    with connection.cursor() as c:
        c.execute(QUERY)
        first = c.query_id
        c.execute(QUERY)
        second = c.query_id
    assert first
    assert second
    assert first != second


def test_query_id_set_statement(connection: Connection) -> None:
    """A SET is applied client-side, so it has no query_id of its own."""
    with connection.cursor() as c:
        c.execute(QUERY)
        assert c.query_id
        c.execute("SET query_label = 'query_id_test'")
        assert c.query_id == ""


def test_query_id_insert(
    connection: Connection, create_drop_test_table_setup_teardown: Callable
) -> None:
    """A statement without a result set still reports its query_id."""
    table = create_drop_test_table_setup_teardown
    label = _label("test_query_id_insert")
    insert = f"INSERT INTO \"{table}\" VALUES (1, 'a')"
    with connection.cursor() as c:
        c.execute(f"SET query_label = '{label}'")
        c.execute(insert)
        query_id = c.query_id
    assert query_id
    assert _history_query_ids(connection, label, insert) == [query_id]


def test_query_id_failed_query(connection: Connection) -> None:
    """A failed query reports the id the engine recorded for it."""
    label = _label("test_query_id_failed")
    with connection.cursor() as c:
        c.execute(f"SET query_label = '{label}'")
        with raises(FireboltError):
            c.execute(FAILING_QUERY)
        query_id = c.query_id
    assert query_id
    assert _history_query_ids(connection, label, FAILING_QUERY, failed=True) == [
        query_id
    ]


def test_query_id_failed_statement_of_several(connection: Connection) -> None:
    """When a later statement fails, query_id is not the id of an earlier one."""
    label = _label("test_query_id_failed_second")
    with connection.cursor() as c:
        c.execute(f"SET query_label = '{label}'")
        with raises(FireboltError):
            c.execute(f"{QUERY}; {FAILING_QUERY}")
        query_id = c.query_id
    assert query_id
    assert query_id not in _history_query_ids(connection, label, QUERY)
    assert _history_query_ids(connection, label, FAILING_QUERY, failed=True) == [
        query_id
    ]


def test_query_id_failed_statement_stream(connection: Connection) -> None:
    """A streamed statement that fails reports its own id."""
    label = _label("test_query_id_failed_stream")
    with connection.cursor() as c:
        c.execute(f"SET query_label = '{label}'")
        # The error surfaces from execute_stream or from nextset, depending on
        # whether the engine reports it in the HTTP status or in the stream
        with raises(FireboltError):
            c.execute_stream(f"{QUERY}; {FAILING_QUERY}")
            while c.nextset():
                pass
        query_id = c.query_id
    assert query_id
    assert _history_query_ids(connection, label, FAILING_QUERY, failed=True) == [
        query_id
    ]


# Firebolt Core has no server-side async execution
@mark.parametrize("connection_factory", ["remote"], indirect=True)
def test_query_id_execute_async(
    connection: Connection, create_drop_test_table_setup_teardown: Callable
) -> None:
    """A server-side async query has no query_id, only a token."""
    table = create_drop_test_table_setup_teardown
    with connection.cursor() as c:
        c.execute(QUERY)
        assert c.query_id
        c.execute_async(f"INSERT INTO \"{table}\" VALUES (1, 'a')")
        assert c.async_query_token
        assert c.query_id == ""
