import time

from pytest import raises

from firebolt.db import Connection
from firebolt.utils.exception import QueryTimeoutError

from .test_queries import LONG_SELECT


def test_query_timeout(connection: Connection):
    timestamp = int(time.time())
    label = f"test_query_async_timeout_cancel:{timestamp}"
    with connection.cursor() as cursor:
        with raises(QueryTimeoutError):
            cursor.execute(f"SET query_label='{label}'")
            cursor.execute(LONG_SELECT, timeout_seconds=1)
        time.sleep(10)  # it takes some time for query history to update
        cursor.execute("SET query_label=''")
        cursor.execute(
            "SELECT status FROM information_schema.engine_query_history WHERE query_label = ? ORDER BY end_time DESC",
            [label],
        )
        status = cursor.fetchone()
        assert status[0] == "CANCELED_EXECUTION"
