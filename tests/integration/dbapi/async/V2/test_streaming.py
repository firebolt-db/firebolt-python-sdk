import os
from typing import List

import psutil
from integration.dbapi.utils import assert_deep_eq
from pytest import raises

from firebolt.async_db import Connection
from firebolt.common._types import ColType
from firebolt.common.row_set.json_lines import Column
from firebolt.utils.exception import FireboltStructuredError


async def test_streaming_select(
    connection: Connection,
    all_types_query: str,
    all_types_query_description: List[Column],
    all_types_query_response: List[ColType],
    timezone_name: str,
) -> None:
    """Select handles all data types properly."""
    async with connection.cursor() as c:
        # For timestamptz test
        assert (
            await c.execute(f"SET time_zone={timezone_name}") == -1
        ), "Invalid set statment row count"

        await c.execute_stream(all_types_query)
        assert c.rowcount == -1, "Invalid rowcount value"
        data = await c.fetchall()
        assert len(data) == c.rowcount, "Invalid data length"
        assert_deep_eq(data, all_types_query_response, "Invalid data")
        assert c.description == all_types_query_description, "Invalid description value"
        assert len(data[0]) == len(c.description), "Invalid description length"
        assert len(await c.fetchall()) == 0, "Redundant data returned by fetchall"
        assert c.rowcount == len(data), "Invalid rowcount value"

        # Different fetch types
        await c.execute_stream(all_types_query)
        assert (
            await c.fetchone() == all_types_query_response[0]
        ), "Invalid fetchone data"
        assert await c.fetchone() is None, "Redundant data returned by fetchone"

        await c.execute_stream(all_types_query)
        assert len(await c.fetchmany(0)) == 0, "Invalid data size returned by fetchmany"
        data = await c.fetchmany()
        assert len(data) == 1, "Invalid data size returned by fetchmany"
        assert_deep_eq(
            data, all_types_query_response, "Invalid data returned by fetchmany"
        )


async def test_streaming_multiple_records(
    connection: Connection,
) -> None:
    """Select handles multiple records properly."""
    row_count, value = (
        100000,
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    )
    sql = f"select '{value}' from generate_series(1, {row_count})"

    async with connection.cursor() as c:
        await c.execute_stream(sql)
        assert c.rowcount == -1, "Invalid rowcount value before fetching"
        async for row in c:
            assert len(row) == 1, "Invalid row length"
            assert row[0] == value, "Invalid row value"
        assert c.rowcount == row_count, "Invalid rowcount value after fetching"


def get_process_memory_mb() -> float:
    """Get the current process memory usage in MB."""
    return psutil.Process(os.getpid()).memory_info().rss / (1024**2)


# @mark.slow
async def test_streaming_limited_memory(
    connection: Connection,
) -> None:

    memory_overhead_threshold_mb = 100
    row_count, value = (
        10000000,
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    )
    original_memory_mb = get_process_memory_mb()
    sql = f"select '{value}' from generate_series(1, {row_count})"
    async with connection.cursor() as c:
        await c.execute_stream(sql)

        memory_diff = get_process_memory_mb() - original_memory_mb
        assert (
            memory_diff < memory_overhead_threshold_mb
        ), f"Memory usage exceeded limit after execution (increased by {memory_diff}MB)"

        assert c.rowcount == -1, "Invalid rowcount value before fetching"
        async for row in c:
            assert len(row) == 1, "Invalid row length"
            assert row[0] == value, "Invalid row value"
        assert c.rowcount == row_count, "Invalid rowcount value after fetching"

        memory_diff = get_process_memory_mb() - original_memory_mb
        assert (
            memory_diff < memory_overhead_threshold_mb
        ), f"Memory usage exceeded limit after fetching results (increased by {memory_diff}MB)"


async def test_streaming_error(
    connection: Connection,
) -> None:
    """Select handles errors properly."""
    sql = (
        "select date(a) from (select '2025-01-01' as a union all select 'invalid' as a)"
    )
    async with connection.cursor() as c:
        with raises(FireboltStructuredError) as e:
            await c.execute_stream(sql)

        assert "Unable to cast TEXT 'invalid' to date" in str(
            e.value
        ), "Invalid error message"
