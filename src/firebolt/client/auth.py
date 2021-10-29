from inspect import cleandoc
from time import time
from typing import Generator, Optional

from httpx import Auth as xAuth
from httpx import Request, Response, post

from firebolt.client.constants import (
    _REQUEST_ERRORS,
    API_REQUEST_TIMEOUT_SECONDS,
    DEFAULT_API_URL,
)
from firebolt.common.exception import AuthenticationError


class Auth(xAuth):
    cleandoc(
        """
        Authentication class for Firebolt database. Get's authentication token using
        provided credentials and updates it when it expires
        """
    )

    __slots__ = (
        "username",
        "password",
        "api_url",
        "_token",
        "_expires",
    )

    def __init__(
        self, username: str, password: str, api_endpoint: str = DEFAULT_API_URL
    ):
        self.username = username
        self.password = password
        # Add schema to url if it's missing
        self._api_endpoint = (
            api_endpoint
            if api_endpoint.startswith("http")
            else f"https://{api_endpoint}"
        )
        self._token: Optional[str] = None
        self._expires: Optional[int] = None

    def copy(self) -> "Auth":
        return Auth(self.username, self.password, self._api_endpoint)

    @property
    def token(self) -> Optional[str]:
        if not self._token or self.expired:
            self.get_new_token()
        return self._token

    @property
    def expired(self) -> Optional[int]:
        return self._expires is not None and self._expires <= int(time())

    def get_new_token(self) -> None:
        """Get new token using username and password"""
        try:
            response = post(
                f"{self._api_endpoint}/auth/v1/login",
                headers={"Content-Type": "application/json;charset=UTF-8"},
                json={"username": self.username, "password": self.password},
                timeout=API_REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()

            parsed = response.json()
            self._check_response_error(parsed)

            self._token = parsed["access_token"]
            self._expires = int(time()) + int(parsed["expires_in"])
        except _REQUEST_ERRORS as e:
            raise AuthenticationError(repr(e), self._api_endpoint)

    def auth_flow(self, request: Request) -> Generator[Request, Response, None]:
        """Add authorization token to request headers. Overrides httpx.Auth.auth_flow"""
        request.headers["Authorization"] = f"Bearer {self.token}"
        yield request

    def _check_response_error(self, response: dict) -> None:
        if "error" in response:
            raise AuthenticationError(
                response.get("message", "unknown server error"),
                self._api_endpoint,
            )
