from typing import Callable
from unittest.mock import MagicMock, patch

from pytest import raises
from pytest_httpx import HTTPXMock

from firebolt.client.auth import FireboltCore
from firebolt.client.auth.base import FireboltAuthVersion
from firebolt.db import connect
from firebolt.utils.exception import ConfigurationError


def test_sync_connect_with_incompatible_params():
    """Test that sync connect rejects incompatible parameters with FireboltCore."""
    with patch("firebolt.db.connection.connect_core") as mock_connect_core:

        # Create a mock FireboltCore auth that returns the correct version
        mock_auth = MagicMock()
        mock_auth.get_firebolt_version.return_value = FireboltAuthVersion.CORE

        # Test with account_name
        with raises(ConfigurationError, match="'account_name' are not compatible"):
            connect(auth=mock_auth, account_name="test_account")

        # Test with engine_name
        with raises(ConfigurationError, match="'engine_name' are not compatible"):
            connect(auth=mock_auth, engine_name="test_engine")

        # Test with engine_url
        with raises(ConfigurationError, match="'engine_url' are not compatible"):
            connect(auth=mock_auth, engine_url="https://example.com")

        # Test with multiple incompatible parameters
        with raises(ConfigurationError, match="'account_name', 'engine_name'"):
            connect(
                auth=mock_auth, account_name="test_account", engine_name="test_engine"
            )

        # Verify connect_core is not called in any of these cases
        mock_connect_core.assert_not_called()

        # Test with compatible parameters
        connect(auth=mock_auth, database="test_db")
        mock_connect_core.assert_called_once()


def test_firebolt_core_no_requests(httpx_mock: HTTPXMock):
    """Test that FireboltCore auth class doesn't send any requests during initialization."""
    # Create FireboltCore auth, no requests should be sent
    FireboltCore()

    # Verify no requests were made
    assert len(httpx_mock.get_requests()) == 0


def test_core_connection_single_query_request(
    httpx_mock: HTTPXMock, select_one_query_callback: Callable
):
    """Test that a FireboltCore connection only makes a single request when running a query."""

    httpx_mock.add_callback(select_one_query_callback)

    # Create auth and connection
    auth = FireboltCore()

    # Connect and run a query
    with connect(auth=auth, database="test_db") as connection:
        cursor = connection.cursor()
        cursor.execute("SELECT 1")

    # Verify exactly one request was made
    requests = httpx_mock.get_requests()
    assert len(requests) == 1

    # Verify the request was for the query execution
    request = requests[0]
    assert request.method == "POST"
    assert "SELECT 1" in request.content.decode()
