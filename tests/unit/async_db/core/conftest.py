"""Fixtures for Async Firebolt Core unit tests."""

from pytest import fixture

from firebolt.client.auth import FireboltCore


@fixture
def core_auth() -> FireboltCore:
    """FireboltCore auth object for unit tests."""
    return FireboltCore()
