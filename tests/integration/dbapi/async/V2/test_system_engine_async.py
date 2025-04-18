import re
from typing import List

from pytest import raises

from firebolt.async_db import Connection
from firebolt.common._types import ColType
from firebolt.common.row_set.types import Column
from firebolt.utils.exception import FireboltStructuredError
from tests.integration.dbapi.utils import assert_deep_eq

system_error_no_db_pattern = re.compile(
    r"The object you're trying to access is not an organization-wide or account-level object.*"
)
system_error_pattern = re.compile(
    r"system engine doesn't support .* statements. Run this statement on a user engine."
)


async def test_system_engine(
    connection_system_engine: Connection,
    all_types_query: str,
    all_types_query_description: List[Column],
    all_types_query_system_engine_response: List[ColType],
    timezone_name: str,
) -> None:
    async with connection_system_engine.cursor() as c:
        assert await c.execute(all_types_query) == 1, "Invalid row count returned"
        assert c.rowcount == 1, "Invalid rowcount value"
        data = await c.fetchall()
        assert len(data) == c.rowcount, "Invalid data length"
        assert_deep_eq(data, all_types_query_system_engine_response, "Invalid data")
        assert c.description == all_types_query_description, "Invalid description value"
        assert len(data[0]) == len(c.description), "Invalid description length"
        assert len(await c.fetchall()) == 0, "Redundant data returned by fetchall"

        # Different fetch types
        await c.execute(all_types_query)
        assert (
            await c.fetchone() == all_types_query_system_engine_response[0]
        ), "Invalid fetchone data"
        assert await c.fetchone() is None, "Redundant data returned by fetchone"

        await c.execute(all_types_query)
        assert len(await c.fetchmany(0)) == 0, "Invalid data size returned by fetchmany"
        data = await c.fetchmany()
        assert len(data) == 1, "Invalid data size returned by fetchmany"
        assert_deep_eq(
            data,
            all_types_query_system_engine_response,
            "Invalid data returned by fetchmany",
        )

        await c.execute("show databases")
        await c.execute("show tables")
        with raises(FireboltStructuredError) as e:
            # Either one or another query fails if we're not on a user engine
            await c.execute('create table if not exists "test_async"(id int)')
            await c.execute('insert into "test_async" values (1)')
        pattern = (
            system_error_pattern
            if connection_system_engine.init_parameters.get("database")
            else system_error_no_db_pattern
        )
        assert pattern.search(str(e.value)), "Invalid error message"


async def test_system_engine_no_db(
    connection_system_engine_no_db: Connection,
    all_types_query: str,
    all_types_query_description: List[Column],
    all_types_query_system_engine_response: List[ColType],
    timezone_name: str,
) -> None:
    """Connecting with engine name is handled properly."""
    await test_system_engine(
        connection_system_engine_no_db,
        all_types_query,
        all_types_query_description,
        all_types_query_system_engine_response,
        timezone_name,
    )


async def test_system_engine_use_engine(
    connection_system_engine: Connection, database_name: str, engine_name: str
):
    table_name = "test_table_async"
    async with connection_system_engine.cursor() as cursor:
        await cursor.execute(f'USE DATABASE "{database_name}"')
        await cursor.execute(f'USE ENGINE "{engine_name}"')
        await cursor.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" (id int)')
        # This query fails if we're not on a user engine
        await cursor.execute(f'INSERT INTO "{table_name}" VALUES (1)')
        await cursor.execute('USE ENGINE "system"')
        # Werify we've switched to system by making previous query fail
        with raises(FireboltStructuredError):
            await cursor.execute(f'INSERT INTO "{table_name}" VALUES (1)')
        await cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')
