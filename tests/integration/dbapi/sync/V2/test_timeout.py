from integration.dbapi.sync.V2.test_queries import LONG_SELECT
from pytest import raises

from firebolt.db import Connection
from firebolt.utils.exception import QueryTimeoutError


def test_query_timeout(connection: Connection):
    with connection.cursor() as cursor:
        with raises(QueryTimeoutError):
            cursor.execute(LONG_SELECT, timeout_seconds=1)
