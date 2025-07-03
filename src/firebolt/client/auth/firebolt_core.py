from typing import AsyncGenerator, Generator, Optional

from httpx import Request, Response

from firebolt.client.auth.base import Auth, FireboltAuthVersion
from firebolt.utils.firebolt_core import get_firebolt_core_engine_url


class FireboltCore(Auth):
    """Authentication class for Firebolt Core.

    Represents authentication for local/remote Docker-based deployments of Firebolt,
    which do not require authentication.

    Args:
        url (str, optional): URL in format protocol://host:port.
            Protocol defaults to http, host defaults to localhost, port
            defaults to 3473.

    Attributes:
        url (str): The parsed URL for the Firebolt Core instance.
        protocol (str): The protocol (http or https).
        host (str): The host (IPv4, IPv6, hostname, or localhost).
        port (int): The port number.
    """

    __slots__ = (
        "url",
        "_token",
        "_expires",
        "_use_token_cache",
    )

    def __init__(self, url: Optional[str] = None):
        # Parse URL and build the complete URL
        self.url = get_firebolt_core_engine_url(url)

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
        return FireboltCore(self.url)

    def get_firebolt_version(self) -> FireboltAuthVersion:
        """Get Firebolt version from auth.

        Returns:
            FireboltAuthVersion: CORE for Firebolt Core authentication
        """
        return FireboltAuthVersion.CORE

    def get_new_token_generator(self) -> Generator[Request, Response, None]:
        """FireboltCore doesn't need token authentication.

        Yields:
            No requests are yielded for FireboltCore

        Raises:
            No exceptions are raised
        """
        # FireboltCore doesn't need to fetch a token
        # This is required by the Auth interface but will never be called
        # with real requests, so we make it return an empty generator
        if False:  # pragma: no cover
            yield

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
