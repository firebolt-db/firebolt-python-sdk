from pytest import mark

from firebolt.service.manager import ResourceManager
from firebolt.service.types_v1 import EngineStatusSummary


def make_engine_name(database_name: str, suffix: str) -> str:
    return f"{database_name}_{suffix}"


def test_get_engine(resource_manager: ResourceManager, engine_name: str):
    engine = resource_manager.engines.get_by_name(engine_name)
    assert (
        engine.current_status_summary
        == EngineStatusSummary.ENGINE_STATUS_SUMMARY_RUNNING
    )


@mark.skip(reason="Interferes with other tests")
def test_engine_stop_start(resource_manager: ResourceManager, engine_name: str):
    engine = resource_manager.engines.get_by_name(engine_name)
    engine.stop(wait_for_stop=True)
    assert (
        engine.current_status_summary
        == EngineStatusSummary.ENGINE_STATUS_SUMMARY_STOPPED
    )
    engine.start(wait_for_start=True)
    assert (
        engine.current_status_summary
        == EngineStatusSummary.ENGINE_STATUS_SUMMARY_RUNNING
    )
