from pytest import raises

from firebolt.async_db import Connection
from firebolt.utils.exception import QueryTimeoutError

# Cannot have absolute path here since interpreter treats word async as keyword and not as a package name
from .test_queries_async import LONG_SELECT


async def test_query_timeout(connection: Connection):
    async with connection.cursor() as cursor:
        with raises(QueryTimeoutError):
            await cursor.execute(LONG_SELECT, timeout_seconds=1)
