from firebolt.common import Settings
from firebolt.service.manager import ResourceManager


def test_database_get_default_engine(
    rm_settings: Settings,
    database_name: str,
    stopped_engine_name: str,
    engine_name: str,
):
    """
    Checks that the default engine is either running or stopped engine
    """
    rm = ResourceManager(rm_settings)

    db = rm.databases.get_by_name(database_name)

    engine = db.get_default_engine()
    assert engine is not None, "default engine is None, but shouldn't"
    assert engine.name in [
        stopped_engine_name,
        engine_name,
    ], "Returned default engine name is neither of known engines"
