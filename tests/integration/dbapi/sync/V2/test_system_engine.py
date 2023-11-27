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