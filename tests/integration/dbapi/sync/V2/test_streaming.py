import os
from typing import List

import psutil
from integration.dbapi.utils import assert_deep_eq

from firebolt.async_db import Connection
from firebolt.common._types import ColType
from firebolt.common.row_set.json_lines import Column


def test_streaming_select(
    connection: Connection,
    all_types_query: str,
    all_types_query_description: List[Column],
    all_types_query_response: List[ColType],
    timezone_name: str,
) -> None:
    """Select handles all data types properly."""
    with connection.cursor() as c:
        # For timestamptz test
        assert (
            c.execute(f"SET time_zone={timezone_name}") == -1
        ), "Invalid set statment row count"

        c.execute_stream(all_types_query)
        assert c.rowcount == -1, "Invalid rowcount value"
        data = c.fetchall()
        assert len(data) == c.rowcount, "Invalid data length"
        assert_deep_eq(data, all_types_query_response, "Invalid data")
        assert c.description == all_types_query_description, "Invalid description value"
        assert len(data[0]) == len(c.description), "Invalid description length"
        assert len(c.fetchall()) == 0, "Redundant data returned by fetchall"
        assert c.rowcount == len(data), "Invalid rowcount value"

        # Different fetch types
        c.execute_stream(all_types_query)
        assert c.fetchone() == all_types_query_response[0], "Invalid fetchone data"
        assert c.fetchone() is None, "Redundant data returned by fetchone"

        c.execute_stream(all_types_query)
        assert len(c.fetchmany(0)) == 0, "Invalid data size returned by fetchmany"
        data = c.fetchmany()
        assert len(data) == 1, "Invalid data size returned by fetchmany"
        assert_deep_eq(
            data, all_types_query_response, "Invalid data returned by fetchmany"
        )


def test_streaming_multiple_records(
    connection: Connection,
) -> None:
    """Select handles multiple records properly."""
    row_count, value = (
        100000,
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    )
    sql = f"select '{value}' from generate_series(1, {row_count})"

    with connection.cursor() as c:
        c.execute_stream(sql)
        assert c.rowcount == -1, "Invalid rowcount value before fetching"
        for row in c:
            assert len(row) == 1, "Invalid row length"
            assert row[0] == value, "Invalid row value"
        assert c.rowcount == row_count, "Invalid rowcount value after fetching"


def get_process_memory_mb() -> float:
    """Get the current process memory usage in MB."""
    return psutil.Process(os.getpid()).memory_info().rss / (1024**2)


# @mark.slow
def test_streaming_limited_memory(
    connection: Connection,
) -> None:

    row_count, value = (
        1000000,
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    )
    original_memory_mb = get_process_memory_mb()
    sql = f"select '{value}' from generate_series(1, {row_count})"
    with connection.cursor() as c:
        c.execute_stream(sql)

        memory_diff = get_process_memory_mb() - original_memory_mb
        assert (
            memory_diff < 10
        ), f"Memory usage exceeded limit after execution (increased by {memory_diff}MB)"

        assert c.rowcount == -1, "Invalid rowcount value before fetching"
        for row in c:
            assert len(row) == 1, "Invalid row length"
            assert row[0] == value, "Invalid row value"
        assert c.rowcount == row_count, "Invalid rowcount value after fetching"

        memory_diff = get_process_memory_mb() - original_memory_mb
        assert (
            memory_diff < 10
        ), f"Memory usage exceeded limit after fetching results (increased by {memory_diff}MB)"
