import time
from random import randint
from typing import Callable

from firebolt.db import Connection

LONG_SELECT = "SELECT checksum(*) FROM GENERATE_SERIES(1, 2500000000)"  # approx 3 sec


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
    connection_factory: Callable[..., Connection]
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
