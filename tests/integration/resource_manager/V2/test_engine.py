from collections import namedtuple

from pytest import mark, raises

from firebolt.client.auth import Auth
from firebolt.service.manager import ResourceManager
from firebolt.service.V2.types import EngineStatus


@mark.slow
def test_create_start_stop_engine(
    auth: Auth,
    account_name: str,
    api_endpoint: str,
    start_stop_engine_name: str,
):
    rm = ResourceManager(
        auth=auth, account_name=account_name, api_endpoint=api_endpoint
    )
    name = start_stop_engine_name
    spec = rm.instance_types.get("S")

    engine = rm.engines.create(
        name=name,
        spec=spec,
        scale=1,
        auto_stop=120,
    )
    assert engine.name == name
    database = None

    try:
        database = rm.databases.create(name=name)
        assert database.name == name

        engine.attach_to_database(database)
        assert engine.database == database

        engine.start()
        assert engine.current_status == EngineStatus.RUNNING

        engine.stop()
        assert engine.current_status in {
            EngineStatus.STOPPING,
            EngineStatus.STOPPED,
        }

    finally:
        # Engine needs to be deleted first
        if engine:
            engine.stop()
            engine.delete()
        if database:
            database.delete()


ParamValue = namedtuple("ParamValue", "set expected")

# we don't include auto_stop since it cannot be
# simultaneously updated with spec and scale
ENGINE_UPDATE_PARAMS = {
    "scale": ParamValue(3, 3),
    "spec": ParamValue("S", "S"),
}


def test_engine_update_single_parameter(
    auth: Auth,
    account_name: str,
    api_endpoint: str,
    database_name: str,
    single_param_engine_name: str,
):
    rm = ResourceManager(
        auth=auth, account_name=account_name, api_endpoint=api_endpoint
    )

    name = single_param_engine_name
    engine = rm.engines.create(name=name)

    try:
        engine.attach_to_database(rm.databases.get(database_name))
        assert engine.database.name == database_name

        for param, value in ENGINE_UPDATE_PARAMS.items():
            engine.update(**{param: value.set})

            engine_new = rm.engines.get(name)
            if param == "spec":
                current_value = (
                    engine_new.spec
                    if isinstance(engine_new.spec, str)
                    else engine_new.spec.name
                )
            elif param == "engine_type":
                current_value = engine_new.type
            else:
                current_value = getattr(engine_new, param)
            assert current_value == value.expected, f"Invalid {param} value"
    finally:
        engine.stop()
        engine.delete()


def test_engine_update_multiple_parameters(
    auth: Auth,
    account_name: str,
    api_endpoint: str,
    database_name: str,
    multi_param_engine_name: str,
):
    rm = ResourceManager(
        auth=auth, account_name=account_name, api_endpoint=api_endpoint
    )

    name = multi_param_engine_name
    engine = rm.engines.create(name=name)

    try:
        engine.attach_to_database(rm.databases.get(database_name))
        assert engine.database.name == database_name

        params = {k: v.set for k, v in ENGINE_UPDATE_PARAMS.items()}
        engine.update(**params)

        engine_new = rm.engines.get(name)

        for param, value in ENGINE_UPDATE_PARAMS.items():
            if param == "spec":
                current_value = (
                    engine_new.spec
                    if isinstance(engine_new.spec, str)
                    else engine_new.spec.name
                )
            elif param == "engine_type":
                current_value = engine_new.type
            else:
                current_value = getattr(engine_new, param)
            assert current_value == value.expected, f"Invalid {param} value"

    finally:
        engine.stop()
        engine.delete()


def test_engine_rename(
    auth: Auth,
    account_name: str,
    api_endpoint: str,
    database_name: str,
    engine_name: str,
):
    rm = ResourceManager(
        auth=auth, account_name=account_name, api_endpoint=api_endpoint
    )
    name = f"{engine_name}_to_rename"
    new_name = f"{engine_name}_renamed"
    engine = rm.engines.create(name=name)

    try:
        with raises(ValueError):
            engine.update(name="name; drop database users")

        engine.update(name=new_name)
        assert engine.name == new_name

        new_engine = rm.engines.get(new_name)
        assert new_engine.name == new_name
    finally:
        engine.stop()
        engine.delete()
