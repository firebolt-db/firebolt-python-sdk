from pytest import raises

from firebolt.client.auth import Auth
from firebolt.service.manager import ResourceManager


def test_databases_get_many(
    auth: Auth,
    account_name: str,
    api_endpoint: str,
    database_name: str,
    engine_name: str,
):
    rm = ResourceManager(
        auth=auth, account_name=account_name, api_endpoint=api_endpoint
    )

    # get all databases, at least one should be returned
    databases = rm.databases.get_many()
    assert len(databases) > 0
    assert database_name in {db.name for db in databases}

    # get all databases, with name_contains
    databases = rm.databases.get_many(name_contains=database_name)
    assert len(databases) > 0
    assert database_name in {db.name for db in databases}

    with raises(ValueError):
        rm.databases.get_many(attached_engine_name_eq=engine_name)

    with raises(ValueError):
        rm.databases.get_many(attached_engine_name_contains=engine_name)

    with raises(ValueError):
        rm.databases.get_many(region_eq="us-west-2")
