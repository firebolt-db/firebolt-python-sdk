from typing import List

from pytest import fixture, mark, raises

from firebolt.common._types import ColType, Column
from firebolt.db import Connection
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
def setup_dbs(connection_system_engine, db_name, second_db_name, engine_name, region):
    with connection_system_engine.cursor() as cursor:

        cursor.execute(create_database(name=db_name))

        cursor.execute(create_engine(engine_name, engine_specs(region)))

        cursor.execute(
            create_database(name=second_db_name, specs=db_specs(region, engine_name))
        )

        yield

        cursor.execute(f"DROP ENGINE IF EXISTS {engine_name}")
        cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
        cursor.execute(f"DROP DATABASE IF EXISTS {second_db_name}")


def test_system_engine(
    connection_system_engine: Connection,
    all_types_query: str,
    all_types_query_description: List[Column],
    all_types_query_system_engine_response: List[ColType],
    timezone_name: str,
) -> None:
    """Connecting with engine name is handled properly."""
    with connection_system_engine.cursor() as c:
        assert c.execute(all_types_query) == 1, "Invalid row count returned"
        assert c.rowcount == 1, "Invalid rowcount value"
        data = c.fetchall()
        assert len(data) == c.rowcount, "Invalid data length"
        assert_deep_eq(data, all_types_query_system_engine_response, "Invalid data")
        assert c.description == all_types_query_description, "Invalid description value"
        assert len(data[0]) == len(c.description), "Invalid description length"
        assert len(c.fetchall()) == 0, "Redundant data returned by fetchall"

        # Different fetch types
        c.execute(all_types_query)
        assert (
            c.fetchone() == all_types_query_system_engine_response[0]
        ), "Invalid fetchone data"
        assert c.fetchone() is None, "Redundant data returned by fetchone"

        c.execute(all_types_query)
        assert len(c.fetchmany(0)) == 0, "Invalid data size returned by fetchmany"
        data = c.fetchmany()
        assert len(data) == 1, "Invalid data size returned by fetchmany"
        assert_deep_eq(
            data,
            all_types_query_system_engine_response,
            "Invalid data returned by fetchmany",
        )

        if connection_system_engine.database:
            c.execute("show tables")
            with raises(OperationalError):
                c.execute("create table test(id int) primary index id")
        else:
            c.execute("show databases")
            with raises(OperationalError):
                c.execute("show tables")


def test_system_engine_no_db(
    connection_system_engine_no_db: Connection,
    all_types_query: str,
    all_types_query_description: List[Column],
    all_types_query_system_engine_response: List[ColType],
    timezone_name: str,
) -> None:
    """Connecting with engine name is handled properly."""
    test_system_engine(
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
def test_query_errors(connection_system_engine, query):
    with connection_system_engine.cursor() as cursor:
        with raises(OperationalError):
            cursor.execute(query)


@mark.xdist_group(name="system_engine")
def test_show_databases(setup_dbs, connection_system_engine, db_name):
    with connection_system_engine.cursor() as cursor:

        cursor.execute("SHOW DATABASES")

        dbs = [row[0] for row in cursor.fetchall()]

        assert db_name in dbs
        assert f"{db_name}_two" in dbs


@mark.xdist_group(name="system_engine")
def test_detach_engine(
    setup_dbs, connection_system_engine, engine_name, second_db_name
):
    def check_engine_exists(cursor, engine_name, db_name):
        cursor.execute("SHOW ENGINES")
        engines = cursor.fetchall()
        # Results have the following columns
        # engine_name, region, spec, scale, status, attached_to, version
        assert engine_name in [row[0] for row in engines]
        assert (engine_name, db_name) in [(row[0], row[5]) for row in engines]

    with connection_system_engine.cursor() as cursor:
        check_engine_exists(cursor, engine_name, db_name=second_db_name)
        cursor.execute(f"DETACH ENGINE {engine_name} FROM {second_db_name}")

        # When engine not attached db is -
        check_engine_exists(cursor, engine_name, db_name="-")

        cursor.execute(f"ATTACH ENGINE {engine_name} TO {second_db_name}")
        check_engine_exists(cursor, engine_name, db_name=second_db_name)


@mark.xdist_group(name="system_engine")
def test_alter_engine(setup_dbs, connection_system_engine, engine_name):
    with connection_system_engine.cursor() as cursor:
        cursor.execute(f"ALTER ENGINE {engine_name} SET AUTO_STOP = 60")

        cursor.execute("SELECT engine_name, auto_stop FROM information_schema.engines")
        engines = cursor.fetchall()
        assert [engine_name, 3600] in engines


@mark.xdist_group(name="system_engine")
def test_start_stop_engine(setup_dbs, connection_system_engine, engine_name):
    def check_engine_status(cursor, engine_name, status):
        cursor.execute("SHOW ENGINES")
        engines = cursor.fetchall()
        # Results have the following columns
        # engine_name, region, spec, scale, status, attached_to, version
        assert engine_name in [row[0] for row in engines]
        assert (engine_name, status) in [(row[0], row[4]) for row in engines]

    with connection_system_engine.cursor() as cursor:
        check_engine_status(cursor, engine_name, "Stopped")
        cursor.execute(f"START ENGINE {engine_name}")
        check_engine_status(cursor, engine_name, "Running")
        cursor.execute(f"STOP ENGINE {engine_name}")
        check_engine_status(cursor, engine_name, "Stopped")
