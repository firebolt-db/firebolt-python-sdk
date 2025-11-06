from typing import Callable, Union
from unittest.mock import MagicMock, patch

from httpx import Request
from pytest import mark, raises
from pytest_httpx import HTTPXMock

from firebolt.model.V2.database import Database
from firebolt.model.V2.engine import Engine, EngineStatus
from firebolt.model.V2.instance_type import InstanceType
from firebolt.service.manager import ResourceManager
from firebolt.utils.exception import EngineNotFoundError
from tests.unit.response import Response
from tests.unit.service.conftest import get_objects_from_db_callback


def create_mock_engine_with_status_transitions(mock_engine: Engine, statuses: list):
    """
    Helper function to create a callback that simulates engine status transitions.

    Args:
        mock_engine: The base engine object to use for creating responses
        statuses: List of EngineStatus values to cycle through on subsequent calls

    Returns:
        A callback function that can be used with HTTPXMock
    """
    call_count = [0]

    def get_engine_callback_with_transitions(request: Request) -> Response:
        # Return different statuses based on call count
        current_status = statuses[min(call_count[0], len(statuses) - 1)]
        call_count[0] += 1

        engine_data = Engine(
            name=mock_engine.name,
            region=mock_engine.region,
            spec=mock_engine.spec,
            scale=mock_engine.scale,
            current_status=current_status,
            version=mock_engine.version,
            endpoint=mock_engine.endpoint,
            warmup=mock_engine.warmup,
            auto_stop=mock_engine.auto_stop,
            type=mock_engine.type,
            _database_name=mock_engine._database_name,
            _service=None,
        )
        return get_objects_from_db_callback([engine_data])(request)

    return get_engine_callback_with_transitions


def test_engine_create(
    httpx_mock: HTTPXMock,
    engine_name: str,
    resource_manager: ResourceManager,
    mock_engine: Engine,
    system_engine_no_db_query_url: str,
):
    def create_engine_callback(request: Request) -> Response:
        if request.content.startswith(b"CREATE"):
            assert (
                request.content.decode("utf-8")
                == f'CREATE ENGINE "{engine_name}" WITH TYPE = M NODES = 2 AUTO_STOP = 7200'
            )
        return get_objects_from_db_callback([mock_engine])(request)

    httpx_mock.add_callback(
        create_engine_callback, url=system_engine_no_db_query_url, is_reusable=True
    )

    for value in (1.0, False, int):
        with raises(TypeError):
            resource_manager.engines.create("wrong", scale=value)

    engine = resource_manager.engines.create(
        name=mock_engine.name,
        spec=mock_engine.spec,
        scale=mock_engine.scale,
        auto_stop=mock_engine.auto_stop,
    )

    assert engine == mock_engine

    for key in ("region", "engine_type", "warmup"):
        with raises(ValueError):
            resource_manager.engines.create(name="failed", **{key: "test"})


def test_engine_not_found(
    httpx_mock: HTTPXMock,
    resource_manager: ResourceManager,
    get_engine_not_found_callback: Callable,
    system_engine_no_db_query_url: str,
):
    httpx_mock.add_callback(
        get_engine_not_found_callback,
        url=system_engine_no_db_query_url,
        is_reusable=True,
    )

    with raises(EngineNotFoundError):
        resource_manager.engines.get("invalid name")


def test_get_connection(
    httpx_mock: HTTPXMock,
    resource_manager: ResourceManager,
    get_engine_callback: Callable,
    database_get_callback: Callable,
    system_engine_no_db_query_url: str,
    system_engine_query_url: str,
    mock_connection_flow: Callable,
    mock_query: Callable,
):
    httpx_mock.add_callback(
        get_engine_callback, url=system_engine_no_db_query_url, is_reusable=True
    )
    httpx_mock.add_callback(
        database_get_callback, url=system_engine_no_db_query_url, is_reusable=True
    )
    mock_connection_flow()
    mock_query()

    engine = resource_manager.engines.get("engine_name")

    with engine.get_connection() as connection:
        connection.cursor().execute("select 1")

    # Some endpoints from connection flow are not used in this test
    httpx_mock.reset()


def test_attach_to_database(
    httpx_mock: HTTPXMock,
    resource_manager: ResourceManager,
    mock_database: Database,
    mock_engine: Engine,
    get_engine_callback: Callable,
    database_get_callback: Callable,
    attach_engine_to_db_callback: Callable,
    system_engine_no_db_query_url: str,
):
    httpx_mock.add_callback(
        database_get_callback, url=system_engine_no_db_query_url, is_reusable=True
    )
    httpx_mock.add_callback(
        get_engine_callback, url=system_engine_no_db_query_url, is_reusable=True
    )
    httpx_mock.add_callback(
        attach_engine_to_db_callback,
        url=system_engine_no_db_query_url,
        is_reusable=True,
    )

    database = resource_manager.databases.get("database")
    engine = resource_manager.engines.get("engine")

    engine._service = resource_manager.engines

    engine.attach_to_database(database)

    assert engine._database_name == database.name


def test_engine_update(
    httpx_mock: HTTPXMock,
    resource_manager: ResourceManager,
    mock_engine: Engine,
    get_engine_callback: Callable,
    update_engine_callback: Callable,
    system_engine_no_db_query_url: str,
    updated_engine_scale: int,
):
    httpx_mock.add_callback(
        get_engine_callback, url=system_engine_no_db_query_url, is_reusable=True
    )
    httpx_mock.add_callback(
        update_engine_callback, url=system_engine_no_db_query_url, is_reusable=True
    )
    httpx_mock.add_callback(
        get_engine_callback, url=system_engine_no_db_query_url, is_reusable=True
    )

    mock_engine._service = resource_manager.engines
    mock_engine.update(scale=updated_engine_scale)

    assert mock_engine.scale == updated_engine_scale

    mock_engine.update(scale=updated_engine_scale)

    assert mock_engine.scale == updated_engine_scale


def test_engine_update_auto_stop_zero(
    httpx_mock: HTTPXMock,
    resource_manager: ResourceManager,
    mock_engine: Engine,
    get_engine_callback: Callable,
    update_engine_callback: Callable,
    system_engine_no_db_query_url: str,
    updated_auto_stop: int,
):
    httpx_mock.add_callback(
        get_engine_callback, url=system_engine_no_db_query_url, is_reusable=True
    )
    httpx_mock.add_callback(
        update_engine_callback, url=system_engine_no_db_query_url, is_reusable=True
    )
    httpx_mock.add_callback(
        get_engine_callback, url=system_engine_no_db_query_url, is_reusable=True
    )

    mock_engine.auto_stop = updated_auto_stop + 100
    # auto_stop = 0 is not considered an empty parameter value
    mock_engine._service = resource_manager.engines
    mock_engine.update(auto_stop=0)

    assert mock_engine.auto_stop == updated_auto_stop


def test_engine_get_by_name(
    httpx_mock: HTTPXMock,
    resource_manager: ResourceManager,
    get_engine_callback: Callable,
    system_engine_no_db_query_url: str,
    mock_engine: Engine,
):
    httpx_mock.add_callback(
        get_engine_callback, url=system_engine_no_db_query_url, is_reusable=True
    )

    engine = resource_manager.engines.get_by_name(mock_engine.name)

    assert engine == mock_engine


@mark.parametrize(
    "engine_status, expected_status",
    [(status.value.upper(), status) for status in EngineStatus],
)
def test_engine_new_status(
    engine_status: str,
    expected_status: EngineStatus,
    httpx_mock: HTTPXMock,
    resource_manager: ResourceManager,
    system_engine_no_db_query_url: str,
    mock_engine: Engine,
):
    mock_engine.current_status = engine_status
    get_engine_callback = get_objects_from_db_callback([mock_engine])

    httpx_mock.add_callback(
        get_engine_callback, url=system_engine_no_db_query_url, is_reusable=True
    )

    engine = resource_manager.engines.get_by_name(mock_engine.name)

    assert engine.current_status == expected_status


@mark.parametrize(
    "spec_input, expected_spec, status_input, expected_status",
    [
        # Test all valid InstanceType values
        ("S", InstanceType.S, "RUNNING", EngineStatus.RUNNING),
        ("M", InstanceType.M, "STOPPED", EngineStatus.STOPPED),
        ("L", InstanceType.L, "STARTING", EngineStatus.STARTING),
        ("XL", InstanceType.XL, "STOPPING", EngineStatus.STOPPING),
        # Test InstanceType enum values directly
        (InstanceType.S, InstanceType.S, "FAILED", EngineStatus.FAILED),
        (InstanceType.M, InstanceType.M, "REPAIRING", EngineStatus.REPAIRING),
        # Test unknown/invalid values that should default to UNKNOWN
        ("INVALID_TYPE", InstanceType.UNKNOWN, "INVALID_STATUS", EngineStatus.UNKNOWN),
        ("XXL", InstanceType.UNKNOWN, "WEIRD_STATE", EngineStatus.UNKNOWN),
        # Test empty strings that should default to UNKNOWN
        ("", InstanceType.UNKNOWN, "", EngineStatus.UNKNOWN),
        # Test all valid EngineStatus values with M instance type
        ("M", InstanceType.M, "STARTED", EngineStatus.STARTED),
        ("M", InstanceType.M, "DROPPING", EngineStatus.DROPPING),
        ("M", InstanceType.M, "DELETING", EngineStatus.DELETING),
        ("M", InstanceType.M, "RESIZING", EngineStatus.RESIZING),
        ("M", InstanceType.M, "DRAINING", EngineStatus.DRAINING),
    ],
)
def test_engine_instantiation_with_different_configurations(
    spec_input: Union[str, InstanceType],
    expected_spec: InstanceType,
    status_input: str,
    expected_status: EngineStatus,
) -> None:
    """
    Test that Engine model correctly handles different instance types and statuses,
    including unknown values and empty strings that should default to UNKNOWN.
    """
    engine = Engine(
        name="test_engine",
        region="us-east-1",
        spec=spec_input,
        scale=2,
        current_status=status_input,
        version="1.0",
        endpoint="https://test.endpoint.com",
        warmup="",
        auto_stop=3600,
        type="general_purpose",
        _database_name="test_db",
        _service=None,
    )

    assert engine.spec == expected_spec
    assert engine.current_status == expected_status
    assert engine.name == "test_engine"
    assert engine.region == "us-east-1"
    assert engine.scale == 2


@patch("time.sleep")
@patch("time.time")
def test_engine_start_waits_for_draining_to_stop(
    mock_time: MagicMock,
    mock_sleep: MagicMock,
    httpx_mock: HTTPXMock,
    resource_manager: ResourceManager,
    mock_engine: Engine,
    system_engine_no_db_query_url: str,
):
    """
    Test that start() waits for an engine in DRAINING state to become STOPPED
    before proceeding with the start operation.
    """
    # Set up time mock to avoid timeout - return incrementing values
    mock_time.return_value = 0  # Always return early time to avoid timeout

    # Set up mock responses: DRAINING -> STOPPED -> STOPPED (after start command)
    callback = create_mock_engine_with_status_transitions(
        mock_engine,
        [
            EngineStatus.DRAINING,  # Initial state
            EngineStatus.STOPPED,  # After first refresh in _wait_for_start_stop
            EngineStatus.STOPPED,  # After start command, final refresh
        ],
    )

    httpx_mock.add_callback(
        callback, url=system_engine_no_db_query_url, is_reusable=True
    )

    # Set up the engine with proper service
    mock_engine._service = resource_manager.engines

    # Call start method
    result = mock_engine.start()

    # Verify that sleep was called (indicating it waited for DRAINING state)
    mock_sleep.assert_called_with(5)

    # Verify the engine is returned
    assert result is mock_engine
    assert result.current_status == EngineStatus.STOPPED


@patch("time.sleep")
@patch("time.time")
def test_engine_start_waits_for_stopping_to_stop(
    mock_time: MagicMock,
    mock_sleep: MagicMock,
    httpx_mock: HTTPXMock,
    resource_manager: ResourceManager,
    mock_engine: Engine,
    system_engine_no_db_query_url: str,
):
    """
    Test that start() waits for an engine in STOPPING state to become STOPPED
    before proceeding with the start operation.
    """
    # Set up time mock to avoid timeout
    mock_time.return_value = 0  # Always return early time to avoid timeout

    # Set up mock responses: STOPPING -> STOPPED -> STOPPED (after start command)
    callback = create_mock_engine_with_status_transitions(
        mock_engine,
        [
            EngineStatus.STOPPING,  # Initial state
            EngineStatus.STOPPED,  # After first refresh in _wait_for_start_stop
            EngineStatus.STOPPED,  # After start command, final refresh
        ],
    )

    httpx_mock.add_callback(
        callback, url=system_engine_no_db_query_url, is_reusable=True
    )

    # Set up the engine with proper service
    mock_engine._service = resource_manager.engines

    # Call start method
    result = mock_engine.start()

    # Verify that sleep was called (indicating it waited for STOPPING state)
    mock_sleep.assert_called_with(5)

    # Verify the engine is returned
    assert result is mock_engine
    assert result.current_status == EngineStatus.STOPPED


@patch("time.sleep")
@patch("time.time")
def test_engine_stop_waits_for_draining_to_stop(
    mock_time: MagicMock,
    mock_sleep: MagicMock,
    httpx_mock: HTTPXMock,
    resource_manager: ResourceManager,
    mock_engine: Engine,
    system_engine_no_db_query_url: str,
):
    """
    Test that stop() waits for an engine in DRAINING state to finish draining
    before proceeding with the stop operation.
    """
    # Set up time mock to avoid timeout
    mock_time.return_value = 0  # Always return early time to avoid timeout

    # Set up mock responses: DRAINING -> RUNNING -> STOPPED (after stop command)
    callback = create_mock_engine_with_status_transitions(
        mock_engine,
        [
            EngineStatus.DRAINING,  # Initial state
            EngineStatus.RUNNING,  # After first refresh in _wait_for_start_stop
            EngineStatus.STOPPED,  # After stop command, final refresh
        ],
    )

    httpx_mock.add_callback(
        callback, url=system_engine_no_db_query_url, is_reusable=True
    )

    # Set up the engine with proper service
    mock_engine._service = resource_manager.engines

    # Call stop method
    result = mock_engine.stop()

    # Verify that sleep was called (indicating it waited for DRAINING state)
    mock_sleep.assert_called_with(5)

    # Verify the engine is returned
    assert result is mock_engine
    assert result.current_status == EngineStatus.STOPPED


@patch("time.sleep")
@patch("time.time")
def test_engine_wait_for_start_stop_timeout(
    mock_time: MagicMock,
    mock_sleep: MagicMock,
    httpx_mock: HTTPXMock,
    resource_manager: ResourceManager,
    mock_engine: Engine,
    system_engine_no_db_query_url: str,
):
    """
    Test that _wait_for_start_stop raises TimeoutError when engine stays in
    transitional state too long.
    """
    # Mock time.time to simulate timeout using a function that tracks calls
    call_count = [0]

    def mock_time_function():
        call_count[0] += 1
        # Return normal time for first few calls, then timeout for _wait_for_start_stop
        if call_count[0] <= 5:
            return 0  # Early time
        else:
            return 3601  # Past timeout

    mock_time.side_effect = mock_time_function

    def get_engine_callback_always_starting(request: Request) -> Response:
        # Always return STARTING to simulate stuck state
        engine_data = Engine(
            name=mock_engine.name,
            region=mock_engine.region,
            spec=mock_engine.spec,
            scale=mock_engine.scale,
            current_status=EngineStatus.STARTING,  # Always starting
            version=mock_engine.version,
            endpoint=mock_engine.endpoint,
            warmup=mock_engine.warmup,
            auto_stop=mock_engine.auto_stop,
            type=mock_engine.type,
            _database_name=mock_engine._database_name,
            _service=None,
        )
        return get_objects_from_db_callback([engine_data])(request)

    httpx_mock.add_callback(
        get_engine_callback_always_starting,
        url=system_engine_no_db_query_url,
        is_reusable=True,
    )

    # Set up the engine with proper service
    mock_engine._service = resource_manager.engines

    # Call start method and expect TimeoutError
    with raises(TimeoutError, match="Excedeed timeout of 3600s waiting for.*starting"):
        mock_engine.start()


@patch("time.sleep")
@patch("time.time")
def test_engine_start_already_running_no_wait(
    mock_time: MagicMock,
    mock_sleep: MagicMock,
    httpx_mock: HTTPXMock,
    resource_manager: ResourceManager,
    mock_engine: Engine,
    system_engine_no_db_query_url: str,
):
    """
    Test that start() doesn't wait when engine is already RUNNING.
    """
    # Mock time to avoid any timeout issues
    mock_time.return_value = 0

    def get_engine_callback_running(request: Request) -> Response:
        engine_data = Engine(
            name=mock_engine.name,
            region=mock_engine.region,
            spec=mock_engine.spec,
            scale=mock_engine.scale,
            current_status=EngineStatus.RUNNING,
            version=mock_engine.version,
            endpoint=mock_engine.endpoint,
            warmup=mock_engine.warmup,
            auto_stop=mock_engine.auto_stop,
            type=mock_engine.type,
            _database_name=mock_engine._database_name,
            _service=None,
        )
        return get_objects_from_db_callback([engine_data])(request)

    httpx_mock.add_callback(
        get_engine_callback_running, url=system_engine_no_db_query_url, is_reusable=True
    )

    # Set up the engine with proper service
    mock_engine._service = resource_manager.engines

    # Call start method
    result = mock_engine.start()

    # Verify that no sleep was called (no waiting happened)
    mock_sleep.assert_not_called()

    # Verify the engine is returned
    assert result is mock_engine
    assert result.current_status == EngineStatus.RUNNING
