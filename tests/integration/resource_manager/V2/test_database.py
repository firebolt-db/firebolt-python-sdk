from pytest import raises

from firebolt.client.auth import Auth
from firebolt.service.manager import ResourceManager


def test_databases_get_many(
    auth: Auth,
    account_name: str,
    api_endpoint: str,
    database_name: str,
    engine_name: str,
    account_version: str,
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

    if account_version == 1:
        # get all databases, with attached engine name equals
        databases = rm.databases.get_many(attached_engine_name_eq=engine_name)
        assert len(databases) > 0
        assert database_name in {db.name for db in databases}

        # get all databases, with attached engine name contains
        databases = rm.databases.get_many(attached_engine_name_contains=engine_name)
        assert len(databases) > 0
        assert database_name in {db.name for db in databases}
    else:
        with raises(ValueError):
            rm.databases.get_many(attached_engine_name_eq=engine_name)

        with raises(ValueError):
            rm.databases.get_many(attached_engine_name_contains=engine_name)

    region = [db for db in databases if db.name == database_name][0].region

    # get all databases, with region_eq
    databases = rm.databases.get_many(region_eq=region)
    assert len(databases) > 0
    assert database_name in {db.name for db in databases}

    # get all databases, with all filters
    kwargs = {
        "name_contains": database_name,
        "region_eq": region,
    }
    if account_version == 1:
        kwargs["attached_engine_name_eq"] = engine_name
        kwargs["attached_engine_name_contains"] = engine_name

    databases = rm.databases.get_many(**kwargs)
    assert len(databases) > 0
    assert database_name in {db.name for db in databases}
