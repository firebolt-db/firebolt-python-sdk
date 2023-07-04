from firebolt.client.auth import Auth
from firebolt.service.manager import ResourceManager


def test_database_get_default_engine(
    auth: Auth,
    account_name: str,
    api_endpoint: str,
    database_name: str,
    stopped_engine_name: str,
    engine_name: str,
):
    """
    Checks that the default engine is either running or stopped engine
    """
    rm = ResourceManager(
        auth=auth, account_name=account_name, api_endpoint=api_endpoint
    )

    db = rm.databases.get(database_name)

    engine = db.get_attached_engines()[0]
    assert engine is not None, "engine is None, but shouldn't be"
    assert engine.name in [
        stopped_engine_name,
        engine_name,
    ], "Returned default engine name is neither of known engines"


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

    # get all databases, with name_contains
    databases = rm.databases.get_many(attached_engine_name_eq=engine_name)
    assert len(databases) > 0
    assert database_name in {db.name for db in databases}

    # get all databases, with name_contains
    databases = rm.databases.get_many(attached_engine_name_contains=engine_name)
    assert len(databases) > 0
    assert database_name in {db.name for db in databases}

    region = [db for db in databases if db.name == database_name][0].region

    # get all databases, with region_eq
    databases = rm.databases.get_many(region_eq=region)
    assert len(databases) > 0
    assert database_name in {db.name for db in databases}

    # get all databases, with all filters
    databases = rm.databases.get_many(
        name_contains=database_name,
        attached_engine_name_eq=engine_name,
        attached_engine_name_contains=engine_name,
        region_eq=region,
    )
    assert len(databases) > 0
    assert database_name in {db.name for db in databases}
