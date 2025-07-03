import pytest

from firebolt.client.auth import FireboltCore


@pytest.fixture(scope="session")
def core_auth() -> FireboltCore:
    """FireboltCore auth object for integration tests."""
    return FireboltCore()
