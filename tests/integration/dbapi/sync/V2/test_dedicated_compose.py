from pytest import mark

from firebolt.db import Connection


@mark.dedicated_core_cluster
@mark.parametrize("connection_factory", ["core"], indirect=True)
def test_dedicated_compose_default(app_setup, connection: Connection):
    """Verify that a dedicated cluster can be brought up with default settings."""
    with connection.cursor() as c:
        c.execute("SELECT 1")
        assert c.fetchone() == [1]


@mark.dedicated_core_cluster({"nodesCount": 2})
@mark.parametrize("connection_factory", ["core"], indirect=True)
def test_dedicated_compose_multi_node(app_setup, connection: Connection):
    """Verify that a dedicated cluster can be brought up with 2 nodes."""
    with connection.cursor() as c:
        c.execute("SELECT 1")
        assert c.fetchone() == [1]
