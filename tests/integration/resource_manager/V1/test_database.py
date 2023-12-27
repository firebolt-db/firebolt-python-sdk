from firebolt.service.manager import ResourceManager


def test_database_get_default_engine(
    resource_manager: ResourceManager,
    database_name: str,
    stopped_engine_name: str,
    engine_name: str,
):
    """
    Checks that the default engine is either running or stopped engine
    """
    db = resource_manager.databases.get_by_name(database_name)

    engine = db.get_default_engine()
    assert engine is not None, "default engine is None, but shouldn't"
    assert engine.name in [
        stopped_engine_name,
        engine_name,
    ], "Returned default engine name is neither of known engines"


def test_create_new_database(resource_manager: ResourceManager, database_name: str):
    new_database_name = database_name + "_rm_test"

    db = resource_manager.databases.create(
        name=new_database_name, description="test database"
    )
    assert db is not None, "new database is None, but shouldn't"
    assert db.name == new_database_name, "new database name doesn't match"

    db.delete()


def test_get_by_id(resource_manager: ResourceManager, database_name: str):
    db = resource_manager.databases.get_by_name(database_name)
    db_id = db.database_id
    assert db_id is not None, "database id is None, but shouldn't"

    test_id = resource_manager.databases.get_id_by_name(database_name)
    assert test_id is not None, "database id is None, but shouldn't"
    assert test_id == db_id, "database id doesn't match"

    db_by_id = resource_manager.databases.get(db_id)
    assert db_by_id is not None, "database by id is None, but shouldn't"
    assert db_by_id.name == database_name, "database by id name doesn't match"


def test_update_description(resource_manager: ResourceManager, database_name: str):
    db = resource_manager.databases.get_by_name(database_name)

    new_description = "new test description"
    db.update(description=new_description)
    assert db.description == new_description, "new description doesn't match"


def test_get_many(resource_manager: ResourceManager, database_name: str):
    dbs = resource_manager.databases.get_many()
    assert len(dbs) > 0, "no databases returned, but shouldn't"
    assert any(db.name == database_name for db in dbs), "database not found"
