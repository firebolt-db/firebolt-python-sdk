from typing import Callable

from pytest import raises

from firebolt.async_db import Connection
from firebolt.utils.exception import QueryTimeoutError
from tests.integration.dbapi.conftest import LONG_SELECT_DEFAULT_V2

# Cannot have absolute path here since interpreter treats word async as keyword and not as a package name
from .test_queries_async import LONG_SELECT


async def test_query_timeout(
    connection: Connection, long_test_value: Callable[[int], int]
) -> None:
    async with connection.cursor() as cursor:
        with raises(QueryTimeoutError):
            await cursor.execute(
                LONG_SELECT.format(long_value=long_test_value(LONG_SELECT_DEFAULT_V2)),
                timeout_seconds=1,
            )
