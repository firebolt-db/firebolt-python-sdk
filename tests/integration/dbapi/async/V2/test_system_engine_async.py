from typing import List

from pytest import raises

from firebolt.async_db import Connection
from firebolt.common._types import ColType, Column
from firebolt.utils.exception import OperationalError
from tests.integration.dbapi.utils import assert_deep_eq


async def test_system_engine(
    connection_system_engine: Connection,
    all_types_query: str,
    all_types_query_description: List[Column],
    all_types_query_system_engine_response: List[ColType],
    timezone_name: str,
) -> None:
    """Connecting with engine name is handled properly."""
    assert (
        await connection_system_engine._client._account_version
    ) == 1, "Invalid account version"
    with connection_system_engine.cursor() as c:
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

        if connection_system_engine.database:
            await c.execute("show tables")
            with raises(OperationalError):
                # Either one or another query fails if we're not on a user engine
                await c.execute("create table if not exists test_async(id int)")
                await c.execute("insert into test values (1)")
        else:
            await c.execute("show databases")
            with raises(OperationalError):
                await c.execute("show tables")


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


async def test_system_engine_v2_account(connection_system_engine_v2: Connection):
    assert (
        await connection_system_engine_v2._client.account_id
    ), "Can't get account id explicitly"
    assert (
        await connection_system_engine_v2._client._account_version
    ) == 2, "Invalid account version"


async def test_system_engine_use_engine(
    connection_system_engine_v2: Connection, setup_v2_db: str, engine_v2: str
):
    table_name = "test_table_async"
    with connection_system_engine_v2.cursor() as cursor:
        await cursor.execute(f"USE DATABASE {setup_v2_db}")
        await cursor.execute(f"USE ENGINE {engine_v2}")
        await cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (id int)")
        # This query fails if we're not on a user engine
        await cursor.execute(f"INSERT INTO {table_name} VALUES (1)")
        await cursor.execute("USE ENGINE system")
        # Werify we've switched to system by making previous query fail
        with raises(OperationalError):
            await cursor.execute(f"INSERT INTO {table_name} VALUES (1)")
        await cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
