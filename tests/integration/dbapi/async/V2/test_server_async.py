import time
from random import randint
from typing import Callable

from pytest import raises

from firebolt.db import Connection
from firebolt.utils.exception import FireboltError, FireboltStructuredError

LONG_SELECT = "SELECT checksum(*) FROM GENERATE_SERIES(1, 2500000000)"  # approx 3 sec


async def test_insert_async(connection: Connection) -> None:
    cursor = connection.cursor()
    rnd_suffix = str(randint(0, 1000))
    table_name = f"test_insert_async_{rnd_suffix}"
    try:
        await cursor.execute(f"CREATE TABLE {table_name} (id INT, name TEXT)")
        await cursor.execute_async(
            f"INSERT INTO {table_name} (id, name) VALUES (1, 'test')"
        )
        token = cursor.async_query_token
        assert token is not None, "Async token was not returned"
        # sleep for 2 sec to make sure the async query is completed
        time.sleep(2)
        assert await connection.is_async_query_running(token) == False
        assert await connection.is_async_query_successful(token) == True
        # Verify the result
        cursor = connection.cursor()
        await cursor.execute(f"SELECT * FROM {table_name}")
        result = await cursor.fetchall()
        assert result == [[1, "test"]]
    finally:
        await cursor.execute(f"DROP TABLE {table_name}")


async def test_insert_async_running(connection: Connection) -> None:
    cursor = connection.cursor()
    rnd_suffix = str(randint(0, 1000))
    table_name = f"test_insert_async_{rnd_suffix}"
    try:
        await cursor.execute(f"CREATE TABLE {table_name} (id LONG)")
        await cursor.execute_async(f"INSERT INTO {table_name} {LONG_SELECT}")
        token = cursor.async_query_token
        assert token is not None, "Async token was not returned"
        assert await connection.is_async_query_running(token) == True
        assert await connection.is_async_query_successful(token) is None
    finally:
        await cursor.execute(f"DROP TABLE {table_name}")


async def test_check_async_execution_from_another_connection(
    connection_factory: Callable[..., Connection]
) -> None:
    connection_1 = await connection_factory()
    connection_2 = await connection_factory()
    cursor = connection_1.cursor()
    rnd_suffix = str(randint(0, 1000))
    table_name = f"test_insert_async_{rnd_suffix}"
    try:
        await cursor.execute(f"CREATE TABLE {table_name} (id INT, name TEXT)")
        await cursor.execute_async(
            f"INSERT INTO {table_name} (id, name) VALUES (1, 'test')"
        )
        token = cursor.async_query_token
        assert token is not None, "Async token was not returned"
        # sleep for 2 sec to make sure the async query is completed
        time.sleep(2)
        assert await connection_2.is_async_query_running(token) == False
        assert await connection_2.is_async_query_successful(token) == True
        # Verify the result
        cursor = connection_2.cursor()
        await cursor.execute(f"SELECT * FROM {table_name}")
        result = await cursor.fetchall()
        assert result == [[1, "test"]]
    finally:
        await cursor.execute(f"DROP TABLE {table_name}")
        await connection_1.aclose()
        await connection_2.aclose()


async def test_check_async_query_fails(connection: Connection) -> None:
    cursor = connection.cursor()
    rnd_suffix = str(randint(0, 1000))
    table_name = f"test_insert_async_{rnd_suffix}"
    try:
        await cursor.execute(f"CREATE TABLE {table_name} (id LONG)")
        await cursor.execute_async(f"INSERT INTO {table_name} VALUES ('string')")
        token = cursor.async_query_token
        assert token is not None, "Async token was not returned"
        # sleep for 2 sec to make sure the async query is completed
        time.sleep(2)
        assert await connection.is_async_query_running(token) == False
        assert await connection.is_async_query_successful(token) == False
    finally:
        await cursor.execute(f"DROP TABLE {table_name}")


async def test_check_async_execution_fails(connection: Connection) -> None:
    cursor = connection.cursor()
    with raises(FireboltStructuredError):
        await cursor.execute_async(f"MALFORMED QUERY")
    with raises(FireboltError):
        cursor.async_query_token
