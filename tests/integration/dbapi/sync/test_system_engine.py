from pytest import fixture, mark, raises

from firebolt.utils.exception import OperationalError


@fixture
def db_name():
    return "ecosystem_test_db"


@fixture
def region():
    return "us-east-1"


@fixture
def engine_name():
    return "ecosystem_test_engine"


@mark.parametrize(
    "query",
    ["SELECT 1", "CREATE DIMENSION TABLE dummy(id INT)", "SHOW TABLES", "SHOW INDEXES"],
)
def test_query_errors(connection_system_engine, query):
    with connection_system_engine.cursor() as cursor:
        with raises(OperationalError):
            cursor.execute(query)


def db_specs(region, attached_engine):
    return (
        f"REGION = '{region}' "
        f"ATTACHED_ENGINES = ('{attached_engine}') "
        "DESCRIPTION = 'Sample description'"
    )


def engine_specs(region):
    return f"REGION = '{region}' " "SPEC = 'B1' " "SCALE = 1"


def create_database(name, specs=None):
    query = f"CREATE DATABASE IF NOT EXISTS {name}"
    query += f" WITH {specs}" if specs else ""
    return query


def create_engine(name, specs=None):
    query = f"CREATE ENGINE IF NOT EXISTS {name}"
    query += f" WITH {specs}" if specs else ""
    return query


def test_system_database(connection_system_engine, db_name, engine_name, region):
    with connection_system_engine.cursor() as cursor:
        cursor.execute(create_database(name=db_name))

        cursor.execute(create_engine(engine_name, engine_specs(region)))

        cursor.execute(
            create_database(name=f"{db_name}_two", specs=db_specs(region, engine_name))
        )

        cursor.execute("SHOW DATABASES")

        dbs = [row[0] for row in cursor.fetchall()]

        assert db_name in dbs
        assert f"{db_name}_two" in dbs

        cursor.execute(f"DETACH ENGINE {engine_name} FROM {db_name}_two")

        cursor.execute("SHOW ENGINES")
        engines = cursor.fetchall()
        assert engine_name in [row[0] for row in engines]
        assert (engine_name, "-") in [(row[0], row[5]) for row in engines]

        cursor.execute(f"ATTACH ENGINE {engine_name} TO {db_name}")

        cursor.execute(f"ALTER ENGINE {engine_name} SET SPEC = B2")

        cursor.execute("SHOW ENGINES")
        engines = cursor.fetchall()
        assert (engine_name, "B2") in [(row[0], row[2]) for row in engines]

        cursor.execute(f"START ENGINE {engine_name}")
        cursor.execute(f"STOP ENGINE {engine_name}")

        cursor.execute(f"DROP ENGINE {engine_name}")
        cursor.execute(f"DROP DATABASE {db_name}")
