from unittest.mock import MagicMock, patch

import pytest
from pytest import raises

from firebolt.async_db import connect
from firebolt.client.auth.base import FireboltAuthVersion
from firebolt.utils.exception import ConfigurationError


@pytest.mark.asyncio
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
