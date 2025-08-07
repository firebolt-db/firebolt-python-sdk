from collections import namedtuple
from unittest.mock import MagicMock, patch

from pytest import mark, raises

from firebolt.utils.usage_tracker import (
    CLIENT_MAP,
    DRIVER_MAP,
    detect_connectors,
    get_sdk_properties,
    get_user_agent_header,
)


@patch("firebolt.utils.usage_tracker.python_version", MagicMock(return_value="3.10.1"))
@patch("firebolt.utils.usage_tracker.release", MagicMock(return_value="2.2.1"))
@patch("firebolt.utils.usage_tracker.system", MagicMock(return_value="Linux"))
@patch("firebolt.utils.usage_tracker.__version__", "0.1.1")
def test_get_sdk_properties():
    with patch.dict("firebolt.utils.usage_tracker.modules", {}, clear=True):
        assert ("3.10.1", "0.1.1", "Linux 2.2.1", "") == get_sdk_properties()
    with patch.dict(
        "firebolt.utils.usage_tracker.modules", {"ciso8601": ""}, clear=True
    ):
        assert ("3.10.1", "0.1.1", "Linux 2.2.1", "ciso8601") == get_sdk_properties()


StackItem = namedtuple("StackItem", "function filename")


@patch.dict(
    "firebolt.utils.usage_tracker.modules",
    {"firebolt_cli": MagicMock(__version__="0.1.1")},
)
@patch.dict(
    "firebolt.utils.usage_tracker.modules",
    {"firebolt_db": MagicMock(__version__="0.1.2")},
)
@patch.dict(
    "firebolt.utils.usage_tracker.modules",
    {"firebolt_provider": MagicMock(__version__="0.1.3")},
)
@patch.dict(
    "firebolt.utils.usage_tracker.modules",
    {
        "dbt": MagicMock(),
        "dbt.adapters": MagicMock(),
        "dbt.adapters.firebolt": MagicMock(__version__="0.1.4"),
    },
)
@patch.dict(
    "firebolt.utils.usage_tracker.modules",
    {"redash": MagicMock(__version__="0.2.2")},
)
@patch.dict(
    "firebolt.utils.usage_tracker.modules",
    {"prefect": MagicMock(__version__="0.2.3")},
)
@mark.parametrize(
    "stack,map,expected",
    [
        (
            [
                StackItem("create_connection", "dir1/dir2/firebolt_cli/utils.py"),
                StackItem("dummy", "dummy.py"),
            ],
            CLIENT_MAP,
            {"FireboltCLI": "0.1.1"},
        ),
        (
            [
                StackItem(
                    "create_connection",
                    "my_documents/some_other_dir/firebolt_cli/utils.py",
                )
            ],
            CLIENT_MAP,
            {"FireboltCLI": "0.1.1"},
        ),
        (
            [StackItem("connect", "sqlalchemy/engine/default.py")],
            DRIVER_MAP,
            {"SQLAlchemy": "0.1.2"},
        ),
        (
            [StackItem("establish_connection", "source_firebolt/source.py")],
            CLIENT_MAP,
            {"AirbyteSource": ""},
        ),
        (
            [StackItem("establish_async_connection", "source_firebolt/source.py")],
            CLIENT_MAP,
            {"AirbyteSource": ""},
        ),
        (
            [StackItem("establish_connection", "destination_firebolt/destination.py")],
            CLIENT_MAP,
            {"AirbyteDestination": ""},
        ),
        (
            [
                StackItem(
                    "establish_async_connection", "destination_firebolt/destination.py"
                )
            ],
            CLIENT_MAP,
            {"AirbyteDestination": ""},
        ),
        (
            [StackItem("get_conn", "firebolt_provider/hooks/firebolt.py")],
            CLIENT_MAP,
            {"Airflow": "0.1.3"},
        ),
        (
            [StackItem("open", "dbt/adapters/firebolt/connections.py")],
            CLIENT_MAP,
            {"DBT": "0.1.4"},
        ),
        (
            [StackItem("open", "dbt/adapters/firebolt/connections.py")],
            DRIVER_MAP,
            {},
        ),
        (
            [StackItem("dummy", "root/superset/models/core.py")],
            CLIENT_MAP,
            {"Superset": ""},
        ),
        (
            [StackItem("run_query", "my_redash/redash/query_runner/firebolt.py")],
            CLIENT_MAP,
            {"Redash": "0.2.2"},
        ),
        (
            [StackItem("run", "root/prefect/tasks/firebolt/firebolt.py")],
            CLIENT_MAP,
            {"Prefect": "0.2.3"},
        ),
    ],
)
def test_detect_connectors(stack, map, expected):
    with patch(
        "firebolt.utils.usage_tracker.inspect.stack", MagicMock(return_value=stack)
    ):
        assert detect_connectors(map) == expected


@mark.parametrize(
    "drivers,clients,additional_parameters,expected_string",
    [
        ([], [], None, "PythonSDK/2 (Python 1; Win; ciso)"),
        (
            [("ConnectorA", "0.1.1")],
            [],
            None,
            "PythonSDK/2 (Python 1; Win; ciso) ConnectorA/0.1.1",
        ),
        (
            (("ConnectorA", "0.1.1"), ("ConnectorB", "0.2.0")),
            (),
            None,
            "PythonSDK/2 (Python 1; Win; ciso) ConnectorA/0.1.1 ConnectorB/0.2.0",
        ),
        (
            [("ConnectorA", "0.1.1"), ("ConnectorB", "0.2.0")],
            [],
            None,
            "PythonSDK/2 (Python 1; Win; ciso) ConnectorA/0.1.1 ConnectorB/0.2.0",
        ),
        (
            [("ConnectorA", "0.1.1"), ("ConnectorB", "0.2.0")],
            [("ClientA", "1.0.1")],
            None,
            "ClientA/1.0.1 PythonSDK/2 (Python 1; Win; ciso) ConnectorA/0.1.1 ConnectorB/0.2.0",
        ),
        (
            [],
            [],
            [("connId", "12345"), ("cachedConnId", "67890-memory")],
            "PythonSDK/2 (Python 1; Win; ciso; connId:12345; cachedConnId:67890-memory)",
        ),
        (
            [("ConnectorA", "0.1.1"), ("ConnectorB", "0.2.0")],
            [("ClientA", "1.0.1")],
            [("connId", "12345")],
            "ClientA/1.0.1 PythonSDK/2 (Python 1; Win; ciso; connId:12345) ConnectorA/0.1.1 ConnectorB/0.2.0",
        ),
        (
            [("ConnectorA", "0.1.1"), ("ConnectorB", "0.2.0")],
            [("ClientA", "1.0.1")],
            [("connId", "12345"), ("cachedConnId", "67890-memory")],
            "ClientA/1.0.1 PythonSDK/2 (Python 1; Win; ciso; connId:12345; cachedConnId:67890-memory) ConnectorA/0.1.1 ConnectorB/0.2.0",
        ),
    ],
)
@patch(
    "firebolt.utils.usage_tracker.get_sdk_properties",
    MagicMock(return_value=("1", "2", "Win", "ciso")),
)
def test_user_agent(drivers, clients, additional_parameters, expected_string):
    assert (
        get_user_agent_header(drivers, clients, additional_parameters)
        == expected_string
    )


@mark.parametrize(
    "drivers,clients",
    [
        ([1], []),
        ((("Con1", "v1.1"), ("Con2")), []),
        (("Connector1.1"), ()),
        (
            [],
            [1],
        ),
        ([], (("Con1", "v1.1"), ("Con2"))),
        ((), ("Connector1.1")),
    ],
)
@patch(
    "firebolt.utils.usage_tracker.get_sdk_properties",
    MagicMock(return_value=("1", "2", "Win", "ciso")),
)
def test_incorrect_user_agent(drivers, clients):
    with raises(ValueError):
        get_user_agent_header(drivers, clients)
