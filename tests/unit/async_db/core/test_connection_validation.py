from typing import Callable
from unittest.mock import MagicMock, patch

import pytest
from pytest import raises
from pytest_httpx import HTTPXMock

from firebolt.async_db import connect
from firebolt.client.auth import FireboltCore
from firebolt.client.auth.base import FireboltAuthVersion
from firebolt.utils.exception import ConfigurationError


@pytest.mark.anyio
async def test_async_connect_with_incompatible_params():
    """Test that async connect rejects incompatible parameters with FireboltCore."""
    with patch("firebolt.async_db.connection.connect_core") as mock_connect_core:

        # Create a mock FireboltCore auth that returns the correct version
        mock_auth = MagicMock()
        mock_auth.get_firebolt_version.return_value = FireboltAuthVersion.CORE

        # Test with account_name
        with raises(ConfigurationError, match="'account_name' are not compatible"):
            await connect(auth=mock_auth, account_name="test_account")

        # Test with engine_name
        with raises(ConfigurationError, match="'engine_name' are not compatible"):
            await connect(auth=mock_auth, engine_name="test_engine")

        # Test with engine_url
        with raises(ConfigurationError, match="'engine_url' are not compatible"):
            await connect(auth=mock_auth, engine_url="https://example.com")

        # Test with multiple incompatible parameters
        with raises(ConfigurationError, match="'account_name', 'engine_name'"):
            await connect(
                auth=mock_auth, account_name="test_account", engine_name="test_engine"
            )

        # Verify connect_core is not called in any of these cases
        mock_connect_core.assert_not_called()

        # Test with compatible parameters
        await connect(auth=mock_auth, database="test_db")
        mock_connect_core.assert_called_once()


@pytest.mark.anyio
async def test_firebolt_core_no_requests_async(httpx_mock: HTTPXMock):
    """Test that FireboltCore auth class doesn't send any requests during initialization."""

    # If FireboltCore has an async init or setup, await it here
    # Assuming it's still a standard class init:
    FireboltCore()

    # Verify no requests were made to the mock
    assert len(httpx_mock.get_requests()) == 0


@pytest.mark.anyio
async def test_core_connection_single_query_request(
    httpx_mock: HTTPXMock, select_one_query_callback: Callable
):
    """Test that a FireboltCore connection only makes a single request when running a query."""

    httpx_mock.add_callback(select_one_query_callback)

    # Create auth and connection
    auth = FireboltCore()

    # Connect and run a query
    async with await connect(auth=auth, database="test_db", client_side_lb=True) as connection:
        cursor = connection.cursor()
        await cursor.execute("SELECT 1")

    # Verify exactly one request was made
    requests = httpx_mock.get_requests()
    assert len(requests) == 1

    # Verify the request was for the query execution
    request = requests[0]
    assert request.method == "POST"
    assert "SELECT 1" in request.read().decode()
