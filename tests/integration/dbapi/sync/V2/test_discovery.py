from firebolt.db import connect


def test_core_discovery_connection(core_url: str):
    with connect(host=core_url, ssl_mode="none", database="firebolt") as connection:
        cursor = connection.cursor()
        cursor.execute("SELECT 42")
        assert cursor.fetchone()[0] == 42
