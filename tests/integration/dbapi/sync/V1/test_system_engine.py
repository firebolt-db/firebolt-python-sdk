from pytest import mark, raises

from firebolt.utils.exception import OperationalError


@mark.parametrize(
    "query",
    ['CREATE DIMENSION TABLE "dummy"(id INT)'],
)
def test_query_errors(connection_system_engine, query):
    with connection_system_engine.cursor() as cursor:
        with raises(OperationalError):
            cursor.execute(query)


@mark.xdist_group(name="system_engine")
def test_select_one(connection_system_engine):
    """SELECT statements are supported"""
    with connection_system_engine.cursor() as cursor:
        cursor.execute("SELECT 1")
