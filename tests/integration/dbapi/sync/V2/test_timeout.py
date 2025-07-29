from typing import Callable

from pytest import raises

from firebolt.db import Connection
from firebolt.utils.exception import QueryTimeoutError
from tests.integration.dbapi.conftest import LONG_SELECT_DEFAULT_V2

from .test_queries import LONG_SELECT


def test_query_timeout(
    connection: Connection, long_test_value: Callable[[int], int]
) -> None:
    with connection.cursor() as cursor:
        with raises(QueryTimeoutError):
            cursor.execute(
                LONG_SELECT.format(long_value=long_test_value(LONG_SELECT_DEFAULT_V2)),
                timeout_seconds=1,
            )
