from typing import List

from pytest import fixture, mark, raises

from firebolt.async_db import Connection
from firebolt.common._types import ColType, Column
from firebolt.utils.exception import OperationalError
from tests.integration.dbapi.utils import assert_deep_eq


@fixture
def db_name(database_name):
    return database_name + "_system_test"


@fixture
def second_db_name(database_name):
    return database_name + "_system_test_two"


@fixture
def region():
    return "us-east-1"


@fixture
def engine_name(engine_name):
    return engine_name + "_system_test"


@fixture
async def setup_dbs(
    connection_system_engine, db_name, second_db_name, engine_name, region
):
    with connection_system_engine.cursor() as cursor:

        await cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
        await cursor.execute(f"DROP DATABASE IF EXISTS {second_db_name}")

        await cursor.execute(create_database(name=db_name))

        await cursor.execute(create_engine(engine_name, engine_specs(region)))

        await cursor.execute(
            create_database(name=second_db_name, specs=db_specs(region, engine_name))
        )

        yield

        await cursor.execute(f"DROP ENGINE IF EXISTS {engine_name}")
        await cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
        await cursor.execute(f"DROP DATABASE IF EXISTS {second_db_name}")


async def test_system_engine(
    connection_system_engine: Connection,
    all_types_query: str,
    all_types_query_description: List[Column],
    all_types_query_system_engine_response: List[ColType],
    timezone_name: str,
) -> None:
    """Connecting with engine name is handled properly."""
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
                await c.execute("create table test(id int) primary index id")
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


def engine_specs(region):
    return f"REGION = '{region}' " "SPEC = 'B1' " "SCALE = 1"


def create_database(name, specs=None):
    query = f"CREATE DATABASE {name}"
    query += f" WITH {specs}" if specs else ""
    return query


def create_engine(name, specs=None):
    query = f"CREATE ENGINE {name}"
    query += f" WITH {specs}" if specs else ""
    return query


def db_specs(region, attached_engine):
    return (
        f"REGION = '{region}' "
        f"ATTACHED_ENGINES = ('{attached_engine}') "
        "DESCRIPTION = 'Sample description'"
    )


@mark.parametrize(
    "query",
    ["CREATE DIMENSION TABLE dummy(id INT)"],
)
async def test_query_errors(connection_system_engine, query):
    with connection_system_engine.cursor() as cursor:
        with raises(OperationalError):
            await cursor.execute(query)


@mark.xdist_group(name="system_engine")
async def test_show_databases(setup_dbs, connection_system_engine, db_name):
    with connection_system_engine.cursor() as cursor:

        await cursor.execute("SHOW DATABASES")

        dbs = [row[0] for row in await cursor.fetchall()]

        assert db_name in dbs
        assert f"{db_name}_two" in dbs


@mark.xdist_group(name="system_engine")
async def test_detach_engine(
    setup_dbs, connection_system_engine, engine_name, second_db_name
):
    async def check_engine_exists(cursor, engine_name, db_name):
        await cursor.execute("SHOW ENGINES")
        engines = await cursor.fetchall()
        # Results have the following columns
        # engine_name, region, spec, scale, status, attached_to, version
        assert engine_name in [row[0] for row in engines]
        assert (engine_name, db_name) in [(row[0], row[5]) for row in engines]

    with connection_system_engine.cursor() as cursor:
        await check_engine_exists(cursor, engine_name, db_name=second_db_name)
        await cursor.execute(f"DETACH ENGINE {engine_name} FROM {second_db_name}")

        # When engine not attached db is -
        await check_engine_exists(cursor, engine_name, db_name="-")

        await cursor.execute(f"ATTACH ENGINE {engine_name} TO {second_db_name}")
        await check_engine_exists(cursor, engine_name, db_name=second_db_name)


@mark.xdist_group(name="system_engine")
async def test_alter_engine(setup_dbs, connection_system_engine, engine_name):
    with connection_system_engine.cursor() as cursor:
        await cursor.execute(f"ALTER ENGINE {engine_name} SET AUTO_STOP = 60")

        await cursor.execute(
            "SELECT engine_name, auto_stop FROM information_schema.engines"
        )
        engines = await cursor.fetchall()
        assert [engine_name, 3600] in engines


@mark.xdist_group(name="system_engine")
async def test_start_stop_engine(setup_dbs, connection_system_engine, engine_name):
    async def check_engine_status(cursor, engine_name, status):
        await cursor.execute("SHOW ENGINES")
        engines = await cursor.fetchall()
        # Results have the following columns
        # engine_name, region, spec, scale, status, attached_to, version
        assert engine_name in [row[0] for row in engines]
        assert (engine_name, status) in [(row[0], row[4]) for row in engines]

    with connection_system_engine.cursor() as cursor:
        await check_engine_status(cursor, engine_name, "Stopped")
        await cursor.execute(f"START ENGINE {engine_name}")
        await check_engine_status(cursor, engine_name, "Running")
        await cursor.execute(f"STOP ENGINE {engine_name}")
        await check_engine_status(cursor, engine_name, "Stopped")
