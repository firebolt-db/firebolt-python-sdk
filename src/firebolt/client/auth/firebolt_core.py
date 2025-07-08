from typing import AsyncGenerator, Generator

from httpx import Request, Response

from firebolt.client.auth.base import Auth, FireboltAuthVersion


class FireboltCore(Auth):
    """Authentication class for Firebolt Core.

    Represents authentication for local/remote Docker-based deployments of Firebolt,
    which do not require authentication.
    """

    __slots__ = (
        "_token",
        "_expires",
        "_use_token_cache",
    )

    def __init__(self) -> None:
        # Initialize with no token caching
        super().__init__(use_token_cache=False)

        # FireboltCore doesn't need a token, but we provide an empty one
        # to satisfy the Auth interface requirements
        self._token = ""
        self._expires = None

    def copy(self) -> "FireboltCore":
        """Make another auth object with same URL.

        Returns:
            FireboltCore: Auth object
        """
        return FireboltCore()

    def get_firebolt_version(self) -> FireboltAuthVersion:
        """Get Firebolt version from auth.

        Returns:
            FireboltAuthVersion: CORE for Firebolt Core authentication
        """
        return FireboltAuthVersion.CORE

    def get_new_token_generator(self) -> Generator:
        """FireboltCore doesn't need token authentication.

        Yields:
            No requests are yielded for FireboltCore

        Raises:
            No exceptions are raised
        """
        yield from []  # No requests to yield, as no authentication is needed

    @property
    def token(self) -> str:
        """Get token for Firebolt Core.

        For FireboltCore, this returns an empty string since no auth is needed.

        Returns:
            str: Empty string (no token needed)
        """
        return ""

    def auth_flow(self, request: Request) -> Generator[Request, Response, None]:
        """Override auth flow for Firebolt Core to avoid sending auth headers.

        This implementation ensures no Authorization headers are sent.

        Args:
            request: The request to authenticate

        Yields:
            The request without authentication headers
        """
        # Yield the request without authentication
        yield request

    async def async_auth_flow(
        self, request: Request
    ) -> AsyncGenerator[Request, Response]:
        """Override async auth flow for Firebolt Core to avoid sending auth headers.

        This implementation ensures no Authorization headers are sent.

        Args:
            request: The request to authenticate

        Yields:
            The request without authentication headers
        """
        # Yield the request without authentication
        yield request
