from typing import List

from pytest import raises

from firebolt.common._types import ColType, Column
from firebolt.db import Connection
from firebolt.utils.exception import OperationalError
from tests.integration.dbapi.utils import assert_deep_eq


def test_system_engine(
    connection_system_engine: Connection,
    all_types_query: str,
    all_types_query_description: List[Column],
    all_types_query_system_engine_response: List[ColType],
    timezone_name: str,
) -> None:
    """Connecting with engine name is handled properly."""
    assert (
        connection_system_engine._client._account_version == 1
    ), "Invalid account version"
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


def test_system_engine_v2_account(connection_system_engine_v2: Connection):
    assert (
        connection_system_engine_v2._client.account_id
    ), "Can't get account id explicitly"
    assert (
        connection_system_engine_v2._client._account_version == 2
    ), "Invalid account version"


def test_system_engine_use_engine(
    connection_system_engine_v2: Connection, setup_v2_db: str, engine_v2: str
):
    table_name = "test_table_sync"
    with connection_system_engine_v2.cursor() as cursor:
        cursor.execute(f"USE DATABASE {setup_v2_db}")
        cursor.execute(f"USE ENGINE {engine_v2}")
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (id int)")
        # This query fails if we're not on a user engine
        cursor.execute(f"INSERT INTO {table_name} VALUES (1)")
        cursor.execute("USE ENGINE system")
        # Werify we've switched to system by making previous query fail
        with raises(OperationalError):
            cursor.execute(f"INSERT INTO {table_name} VALUES (1)")
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
