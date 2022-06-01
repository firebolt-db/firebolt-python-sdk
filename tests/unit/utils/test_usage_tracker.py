from collections import namedtuple
from unittest.mock import MagicMock, patch

from pytest import mark

from firebolt.utils.usage_tracker import (
    Format,
    UsageTracker,
    get_sdk_properties,
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
@mark.parametrize(
    "stack,expected",
    [
        (
            [
                StackItem("create_connection", "dir1/dir2/firebolt_cli/utils.py"),
                StackItem("dummy", "dummy.py"),
            ],
            {"FireboltCLI": "0.1.1"},
        ),
        (
            [
                StackItem(
                    "create_connection",
                    "my_documents\\some_other_dir\\firebolt_cli\\utils.py",
                )
            ],
            {"FireboltCLI": "0.1.1"},
        ),
        (
            [StackItem("connect", "sqlalchemy/engine/default.py")],
            {"SQLAlchemy": "0.1.2"},
        ),
        (
            [StackItem("establish_connection", "source_firebolt/source.py")],
            {"AibyteSource": ""},
        ),
        (
            [StackItem("establish_async_connection", "source_firebolt/source.py")],
            {"AibyteSource": ""},
        ),
        (
            [StackItem("establish_connection", "destination_firebolt/destination.py")],
            {"AibyteDestination": ""},
        ),
        (
            [
                StackItem(
                    "establish_async_connection", "destination_firebolt/destination.py"
                )
            ],
            {"AibyteDestination": ""},
        ),
        (
            [StackItem("get_conn", "firebolt_provider/hooks/firebolt.py")],
            {"Airflow": "0.1.3"},
        ),
        ([StackItem("open", "dbt/adapters/firebolt/connections.py")], {"DBT": "0.1.4"}),
    ],
)
def test_detect_connectors(stack, expected):
    with patch(
        "firebolt.utils.usage_tracker.inspect.stack", MagicMock(return_value=stack)
    ):
        assert UsageTracker().connectors == expected


def test_add_connector():
    ut = UsageTracker(("MyAwesomeConnector", "0.1.1"))
    assert "MyAwesomeConnector" in ut.connectors
    assert ut.connectors["MyAwesomeConnector"] == "0.1.1"


def test_add_connector_list():
    connectors = [("MyAwesomeConnector", "0.1.1"), ("MyLessAwesomeConnector", "0.0.1")]
    ut = UsageTracker(connectors)
    for name, version in connectors:
        assert name in ut.connectors
        assert ut.connectors[name] == version


@mark.parametrize(
    "connectors,expected_string",
    [
        (None, "PythonSDK/2 (Python 1; Win; ciso)"),
        (("ConnectorA", "0.1.1"), "PythonSDK/2 (Python 1; Win; ciso) ConnectorA/0.1.1"),
        (
            [("ConnectorA", "0.1.1"), ("ConnectorB", "0.2.0")],
            "PythonSDK/2 (Python 1; Win; ciso) ConnectorA/0.1.1 ConnectorB/0.2.0",
        ),
    ],
)
@patch(
    "firebolt.utils.usage_tracker.get_sdk_properties",
    MagicMock(return_value=("1", "2", "Win", "ciso")),
)
def test_user_agent(connectors, expected_string):
    ut = UsageTracker(connectors)
    assert ut.format(Format.USER_AGENT) == expected_string
