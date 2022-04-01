from time import time
from typing import Generator, Optional

from httpx import Auth as HttpxAuth
from httpx import Request, Response, codes

from firebolt.client.constants import _REQUEST_ERRORS, DEFAULT_API_URL
from firebolt.common.exception import AuthenticationError
from firebolt.common.token_storage import TokenSecureStorage
from firebolt.common.urls import AUTH_URL
from firebolt.common.util import fix_url_schema


class Auth(HttpxAuth):

    """
    Authentication class for Firebolt database. Gets authentication token using
    provided credentials and updates it when it expires
    """

    __slots__ = (
        "username",
        "password",
        "api_url",
        "_token",
        "_expires",
    )

    requires_response_body = True

    @staticmethod
    def from_token(token: str) -> "Auth":
        a = Auth("", "")
        a._token = token
        return a

    def __init__(
        self,
        username: str,
        password: str,
        api_endpoint: str = DEFAULT_API_URL,
    ):
        self.username = username
        self.password = password
        self._token_storage = TokenSecureStorage(username=username, password=password)

        # Add schema to url if it's missing
        self._api_endpoint = fix_url_schema(api_endpoint)
        self._token: Optional[str] = self._token_storage.get_cached_token()
        self._expires: Optional[int] = None

    def copy(self) -> "Auth":
        return Auth(self.username, self.password, self._api_endpoint)

    @property
    def token(self) -> Optional[str]:
        return self._token

    @property
    def expired(self) -> Optional[int]:
        return self._expires is not None and self._expires <= int(time())

    def get_new_token_generator(self) -> Generator[Request, Response, None]:
        """Get new token using username and password"""
        try:
            response = yield Request(
                "POST",
                AUTH_URL.format(api_endpoint=self._api_endpoint),
                headers={
                    "Content-Type": "application/json;charset=UTF-8",
                    "User-Agent": "firebolt-sdk",
                },
                json={"username": self.username, "password": self.password},
            )
            response.raise_for_status()

            parsed = response.json()
            self._check_response_error(parsed)

            self._token = parsed["access_token"]
            self._expires = int(time()) + int(parsed["expires_in"])

            self._token_storage.cache_token(parsed["access_token"], self._expires)

        except _REQUEST_ERRORS as e:
            raise AuthenticationError(repr(e), self._api_endpoint)

    def auth_flow(self, request: Request) -> Generator[Request, Response, None]:
        """Add authorization token to request headers.
        Overrides ``httpx.Auth.auth_flow``"""

        if not self.token or self.expired:
            yield from self.get_new_token_generator()

        request.headers["Authorization"] = f"Bearer {self.token}"

        response = yield request

        if response.status_code == codes.UNAUTHORIZED:
            yield from self.get_new_token_generator()
            request.headers["Authorization"] = f"Bearer {self.token}"
            yield request

    def _check_response_error(self, response: dict) -> None:
        if "error" in response:
            raise AuthenticationError(
                response.get("message", "unknown server error"),
                self._api_endpoint,
            )
