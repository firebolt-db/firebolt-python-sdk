import time

from firebolt.service.manager import ResourceManager
from firebolt.service.types import EngineStatusSummary


def test_create_start_stop_engine():
    rm = ResourceManager()
    name = f"integration_test_{int(time.time())}"

    engine = rm.engines.create(name=name)
    assert engine.name == name

    database = rm.databases.create(name=name)
    assert database.name == name

    engine.attach_to_database(database=database)
    assert engine.database == database

    engine = engine.start()
    assert (
        engine.current_status_summary
        == EngineStatusSummary.ENGINE_STATUS_SUMMARY_RUNNING
    )

    engine = engine.stop()
    assert engine.current_status_summary in {
        EngineStatusSummary.ENGINE_STATUS_SUMMARY_STOPPING,
        EngineStatusSummary.ENGINE_STATUS_SUMMARY_STOPPED,
    }


def test_copy_engine():
    rm = ResourceManager()
    name = f"integration_test_{int(time.time())}"

    engine = rm.engines.create(name=name)
    assert engine.name == name

    engine.name = f"{engine.name}_copy"
    engine_copy = rm.engines._send_create_engine(
        engine=engine,
        engine_revision=rm.engine_revisions.get_engine_revision_by_key(
            engine.latest_revision_key
        ),
    )
    assert engine_copy
