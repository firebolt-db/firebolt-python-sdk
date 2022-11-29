from pytest import fixture, mark, raises

from firebolt.utils.exception import OperationalError


@fixture
def db_name(database_name):
    return database_name + "_system_test"


@fixture
def second_db_name(database_name):
    return database_name + "_system_test_two"


@fixture
def region():
    return "us-east-1"


@fixture
def engine_name(engine_name):
    return engine_name + "_system_test"


@fixture
def setup_dbs(connection_system_engine, db_name, second_db_name, engine_name, region):
    with connection_system_engine.cursor() as cursor:
        cursor.execute(create_database(name=db_name))

        cursor.execute(create_engine(engine_name, engine_specs(region)))

        cursor.execute(
            create_database(name=second_db_name, specs=db_specs(region, engine_name))
        )

        yield

        cursor.execute(f"DROP ENGINE IF EXISTS {engine_name}")
        cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
        cursor.execute(f"DROP DATABASE IF EXISTS {second_db_name}")


def engine_specs(region):
    return f"REGION = '{region}' " "SPEC = 'B1' " "SCALE = 1"


def create_database(name, specs=None):
    query = f"CREATE DATABASE {name}"
    query += f" WITH {specs}" if specs else ""
    return query


def create_engine(name, specs=None):
    query = f"CREATE ENGINE {name}"
    query += f" WITH {specs}" if specs else ""
    return query


def db_specs(region, attached_engine):
    return (
        f"REGION = '{region}' "
        f"ATTACHED_ENGINES = ('{attached_engine}') "
        "DESCRIPTION = 'Sample description'"
    )


@mark.parametrize(
    "query",
    ["SELECT 1", "CREATE DIMENSION TABLE dummy(id INT)", "SHOW TABLES", "SHOW INDEXES"],
)
def test_query_errors(connection_system_engine, query):
    with connection_system_engine.cursor() as cursor:
        with raises(OperationalError):
            cursor.execute(query)


def test_show_databases(setup_dbs, connection_system_engine, db_name):
    with connection_system_engine.cursor() as cursor:

        cursor.execute("SHOW DATABASES")

        dbs = [row[0] for row in cursor.fetchall()]

        assert db_name in dbs
        assert f"{db_name}_two" in dbs


def test_detach_engine(
    setup_dbs, connection_system_engine, engine_name, second_db_name
):
    def check_engine_exists(cursor, engine_name, db_name):
        cursor.execute("SHOW ENGINES")
        engines = cursor.fetchall()
        # Results have the following columns
        # engine_name, region, spec, scale, status, attached_to, version
        assert engine_name in [row[0] for row in engines]
        assert (engine_name, db_name) in [(row[0], row[5]) for row in engines]

    with connection_system_engine.cursor() as cursor:
        check_engine_exists(cursor, engine_name, db_name=second_db_name)
        cursor.execute(f"DETACH ENGINE {engine_name} FROM {second_db_name}")

        # When engine not attached db is -
        check_engine_exists(cursor, engine_name, db_name="-")

        cursor.execute(f"ATTACH ENGINE {engine_name} TO {second_db_name}")
        check_engine_exists(cursor, engine_name, db_name=second_db_name)


def test_alter_engine(setup_dbs, connection_system_engine, engine_name):
    with connection_system_engine.cursor() as cursor:
        cursor.execute(f"ALTER ENGINE {engine_name} SET SPEC = B2")

        cursor.execute("SHOW ENGINES")
        engines = cursor.fetchall()
        assert (engine_name, "B2") in [(row[0], row[2]) for row in engines]


def test_start_stop_engine(setup_dbs, connection_system_engine, engine_name):
    def check_engine_status(cursor, engine_name, status):
        cursor.execute("SHOW ENGINES")
        engines = cursor.fetchall()
        # Results have the following columns
        # engine_name, region, spec, scale, status, attached_to, version
        assert engine_name in [row[0] for row in engines]
        assert (engine_name, status) in [(row[0], row[4]) for row in engines]

    with connection_system_engine.cursor() as cursor:
        check_engine_status(cursor, engine_name, "Stopped")
        cursor.execute(f"START ENGINE {engine_name}")
        check_engine_status(cursor, engine_name, "Running")
        cursor.execute(f"STOP ENGINE {engine_name}")
        check_engine_status(cursor, engine_name, "Stopped")
