from time import time
from typing import Generator

from httpx import Request, Response

from firebolt.client.auth.base import Auth
from firebolt.client.constants import _REQUEST_ERRORS
from firebolt.utils.exception import AuthenticationError


class _RequestBased(Auth):
    """Base abstract class for http request based authentication."""

    def _make_auth_request(self) -> Request:
        """Create an HTTP request required for authentication.

        Returns:
            Request: HTTP request, required for authentication.
        """
        raise NotImplementedError()

    @staticmethod
    def _check_response_error(response: dict) -> None:
        """Check if response data contains errors.

        Args:
            response (dict): Response data

        Raises:
            AuthenticationError: Were unable to authenticate
        """
        if "error" in response:
            raise AuthenticationError(
                response.get("message", "unknown server error"),
            )

    def get_new_token_generator(self) -> Generator[Request, Response, None]:
        """Get new token using username and password.

        Yields:
            Request: An http request to get token. Expects Response to be sent back

        Raises:
            AuthenticationError: Error while authenticating with provided credentials
        """
        try:
            response = yield self._make_auth_request()
            response.raise_for_status()

            parsed = response.json()
            self._check_response_error(parsed)

            self._token = parsed["access_token"]
            self._expires = int(time()) + int(parsed["expires_in"])

        except _REQUEST_ERRORS as e:
            raise AuthenticationError(repr(e)) from e
