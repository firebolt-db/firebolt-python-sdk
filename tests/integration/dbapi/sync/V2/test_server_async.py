import time
from random import randint
from typing import Callable

from pytest import mark, raises

from firebolt.db import Connection
from firebolt.utils.exception import FireboltError, FireboltStructuredError

LONG_SELECT = "SELECT checksum(*) FROM GENERATE_SERIES(1, 2500000000)"  # approx 3 sec

# Async is only supported in remote engines, no core support yet
# Mark all the tests in this module to run only with the "remote" connection
pytestmark = mark.parametrize("connection_factory", ["remote"], indirect=True)


def test_insert_async(connection: Connection) -> None:
    cursor = connection.cursor()
    rnd_suffix = str(randint(0, 1000))
    table_name = f"test_insert_async_{rnd_suffix}"
    try:
        cursor.execute(f"CREATE TABLE {table_name} (id INT, name TEXT)")
        cursor.execute_async(f"INSERT INTO {table_name} (id, name) VALUES (1, 'test')")
        token = cursor.async_query_token
        assert token is not None, "Async token was not returned"
        # sleep for 2 sec to make sure the async query is completed
        time.sleep(2)
        assert connection.is_async_query_running(token) == False
        assert connection.is_async_query_successful(token) == True
        # Verify the result
        cursor = connection.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")
        result = cursor.fetchall()
        assert result == [[1, "test"]]
        info = connection.get_async_query_info(token)
        assert len(info) == 1
        # Verify query id is showing in query history
        for _ in range(3):
            cursor.execute(
                "SELECT 1 FROM information_schema.engine_query_history WHERE status='STARTED_EXECUTION' AND query_id = ?",
                [info[0].query_id],
            )
            query_history_result = cursor.fetchall()
            if len(query_history_result) != 0:
                break
            # Sometimes it takes a while for the query history to be updated
            # so we will retry a few times
            time.sleep(10)
        assert len(query_history_result) == 1
    finally:
        cursor.execute(f"DROP TABLE {table_name}")


def test_insert_async_running(connection: Connection) -> None:
    cursor = connection.cursor()
    rnd_suffix = str(randint(0, 1000))
    table_name = f"test_insert_async_{rnd_suffix}"
    try:
        cursor.execute(f"CREATE TABLE {table_name} (id LONG)")
        cursor.execute_async(f"INSERT INTO {table_name} {LONG_SELECT}")
        token = cursor.async_query_token
        assert token is not None, "Async token was not returned"
        assert connection.is_async_query_running(token) == True
        assert connection.is_async_query_successful(token) is None
    finally:
        cursor.execute(f"DROP TABLE {table_name}")


def test_check_async_execution_from_another_connection(
    connection: Connection,
    connection_factory: Callable[..., Connection],
) -> None:
    connection_1 = connection_factory()
    connection_2 = connection_factory()
    cursor = connection_1.cursor()
    rnd_suffix = str(randint(0, 1000))
    table_name = f"test_insert_async_{rnd_suffix}"
    try:
        cursor.execute(f"CREATE TABLE {table_name} (id INT, name TEXT)")
        cursor.execute_async(f"INSERT INTO {table_name} (id, name) VALUES (1, 'test')")
        token = cursor.async_query_token
        assert token is not None, "Async token was not returned"
        # sleep for 2 sec to make sure the async query is completed
        time.sleep(2)
        assert connection_2.is_async_query_running(token) == False
        assert connection_2.is_async_query_successful(token) == True
        # Verify the result
        cursor = connection_2.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")
        result = cursor.fetchall()
        assert result == [[1, "test"]]
    finally:
        cursor.execute(f"DROP TABLE {table_name}")
        connection_1.close()
        connection_2.close()


def test_check_async_query_fails(connection: Connection) -> None:
    cursor = connection.cursor()
    rnd_suffix = str(randint(0, 1000))
    table_name = f"test_insert_async_{rnd_suffix}"
    try:
        cursor.execute(f"CREATE TABLE {table_name} (id LONG)")
        cursor.execute_async(f"INSERT INTO {table_name} VALUES ('string')")
        token = cursor.async_query_token
        assert token is not None, "Async token was not returned"
        # sleep for 2 sec to make sure the async query is completed
        time.sleep(2)
        assert connection.is_async_query_running(token) == False
        assert connection.is_async_query_successful(token) == False
    finally:
        cursor.execute(f"DROP TABLE {table_name}")


def test_check_async_execution_fails(connection: Connection) -> None:
    cursor = connection.cursor()
    with raises(FireboltStructuredError):
        cursor.execute_async(f"MALFORMED QUERY")
    with raises(FireboltError):
        cursor.async_query_token


def test_cancel_async_query(connection: Connection) -> None:
    cursor = connection.cursor()
    rnd_suffix = str(randint(0, 1000))
    table_name = f"test_insert_async_{rnd_suffix}"
    try:
        cursor.execute(f"CREATE TABLE {table_name} (id LONG)")
        cursor.execute_async(f"INSERT INTO {table_name} {LONG_SELECT}")
        token = cursor.async_query_token
        assert token is not None, "Async token was not returned"
        assert connection.is_async_query_running(token) == True
        connection.cancel_async_query(token)
        assert connection.is_async_query_running(token) == False
        assert connection.is_async_query_successful(token) == False
        cursor.execute(f"SELECT * FROM {table_name}")
        result = cursor.fetchall()
        assert result == []
    finally:
        cursor.execute(f"DROP TABLE {table_name}")
