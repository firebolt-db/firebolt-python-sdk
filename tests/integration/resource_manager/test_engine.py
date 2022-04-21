from collections import namedtuple

import pytest

from firebolt.model.engine import Engine
from firebolt.service.manager import ResourceManager, Settings
from firebolt.service.types import (
    EngineStatusSummary,
    EngineType,
    WarmupMethod,
)


def make_engine_name(database_name: str, suffix: str) -> str:
    return f"{database_name}_{suffix}"


@pytest.mark.skip(reason="manual test")
def test_create_start_stop_engine(database_name: str):
    rm = ResourceManager()
    name = make_engine_name(database_name, "start_stop")

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


@pytest.mark.skip(reason="manual test")
def test_copy_engine(database_name):
    rm = ResourceManager()
    name = make_engine_name(database_name, "copy")

    engine = rm.engines.create(name=name)
    assert engine.name == name

    engine.name = f"{engine.name}_copy"
    engine_copy = rm.engines._send_create_engine(
        engine=engine,
        engine_revision=rm.engine_revisions.get_by_key(engine.latest_revision_key),
    )
    assert engine_copy


def test_databases_get_many(rm_settings: Settings, database_name, engine_name):
    rm = ResourceManager(rm_settings)

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


def get_engine_params(rm: ResourceManager, engine: Engine):
    engine_revision = rm.engine_revisions.get_by_key(engine.latest_revision_key)
    instance_type = rm.instance_types.get_by_key(
        engine_revision.specification.db_compute_instances_type_key
    )

    return {
        "engine_type": engine.settings.preset,
        "scale": engine_revision.specification.db_compute_instances_count,
        "spec": instance_type.name,
        "auto_stop": engine.settings.auto_stop_delay_duration,
        "warmup": engine.settings.warm_up,
        "description": engine.description,
    }


ParamValue = namedtuple("ParamValue", "set expected")
ENGINE_UPDATE_PARAMS = {
    "engine_type": ParamValue(
        EngineType.DATA_ANALYTICS, "ENGINE_SETTINGS_PRESET_DATA_ANALYTICS"
    ),
    "scale": ParamValue(23, 23),
    "spec": ParamValue("B1", "B1"),
    "auto_stop": ParamValue(123, "7380s"),
    "warmup": ParamValue(WarmupMethod.PRELOAD_ALL_DATA, "ENGINE_SETTINGS_WARM_UP_ALL"),
    "description": ParamValue("new db description", "new db description"),
}


def test_engine_update_single_parameter(rm_settings: Settings, database_name: str):
    rm = ResourceManager(rm_settings)

    name = make_engine_name(database_name, "single_param")
    engine = rm.engines.create(name=name)

    engine.attach_to_database(database=rm.databases.get_by_name(database_name))
    assert engine.database.name == database_name

    for param, value in ENGINE_UPDATE_PARAMS.items():
        engine.update(**{param: value.set})

        engine = rm.engines.get_by_name(name)
        new_params = get_engine_params(rm, engine)
        assert new_params[param] == value.expected

    engine.delete()


def test_engine_update_multiple_parameters(rm_settings: Settings, database_name: str):
    rm = ResourceManager(rm_settings)

    name = make_engine_name(database_name, "multi_param")
    engine = rm.engines.create(name=name)

    engine.attach_to_database(database=rm.databases.get_by_name(database_name))
    assert engine.database.name == database_name

    engine.update(
        **dict({(param, value.set) for param, value in ENGINE_UPDATE_PARAMS.items()})
    )

    engine = rm.engines.get_by_name(name)
    new_params = get_engine_params(rm, engine)

    for param, value in ENGINE_UPDATE_PARAMS.items():
        assert new_params[param] == value.expected

    engine.delete()
