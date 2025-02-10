import time

from pytest import raises

from firebolt.async_db import Connection
from firebolt.utils.exception import QueryTimeoutError

# Cannot have absolute path here since interpreter treats word async as keyword and not as a package name
from .test_queries_async import LONG_SELECT


async def test_query_timeout(connection: Connection):
    timestamp = int(time.time())
    label = f"test_query_async_timeout_cancel:{timestamp}"
    with connection.cursor() as cursor:
        with raises(QueryTimeoutError):
            await cursor.execute(f"SET query_label='{label}'")
            await cursor.execute(LONG_SELECT, timeout_seconds=1)
        time.sleep(10)  # it takes some time for query history to update
        await cursor.execute("SET query_label=''")
        await cursor.execute(
            "SELECT status FROM information_schema.engine_query_history WHERE query_label = ? ORDER BY end_time DESC",
            [label],
        )
        status = await cursor.fetchone()
        assert status[0] == "CANCELED_EXECUTION"
