import pytest

from firebolt.client.auth import FireboltCore


@pytest.fixture(scope="session")
def core_url() -> str:
    """URL for Firebolt Core (can be overridden with environment variable)."""
    return "http://localhost:3473"


@pytest.fixture(scope="session")
def core_auth(core_url: str) -> FireboltCore:
    """FireboltCore auth object for integration tests."""
    return FireboltCore(url=core_url)
