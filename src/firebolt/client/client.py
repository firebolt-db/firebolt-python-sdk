import time
import typing
from functools import cached_property, wraps
from inspect import cleandoc
from json import JSONDecodeError
from typing import Any, Optional, Tuple

import httpx
from httpx._types import AuthTypes

from firebolt.common.exception import AuthenticationError

DEFAULT_API_URL: str = "api.app.firebolt.io"
API_REQUEST_TIMEOUT_SECONDS: Optional[int] = 60
_REQUEST_ERRORS: Tuple[type[Exception], ...] = (
    httpx.HTTPError,
    httpx.InvalidURL,
    httpx.CookieConflict,
    httpx.StreamError,
    JSONDecodeError,
    KeyError,
    ValueError,
)


class Auth(httpx.Auth):
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
        return self._expires is not None and self._expires <= int(time.time())

    def get_new_token(self) -> None:
        """Get new token using username and password"""
        try:
            response = httpx.post(
                f"{self._api_endpoint}/auth/v1/login",
                headers={"Content-Type": "application/json;charset=UTF-8"},
                json={"username": self.username, "password": self.password},
                timeout=API_REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()

            parsed = response.json()
            self._check_response_error(parsed)

            self._token = parsed["access_token"]
            self._expires = int(time.time()) + int(parsed["expires_in"])
        except _REQUEST_ERRORS as e:
            raise AuthenticationError(repr(e), self._api_endpoint)

    def auth_flow(
        self, request: httpx.Request
    ) -> typing.Generator[httpx.Request, httpx.Response, None]:
        """Add authorization token to request headers. Overrides httpx.Auth.auth_flow"""
        request.headers["Authorization"] = f"Bearer {self.token}"
        yield request

    def _check_response_error(self, response: dict) -> None:
        if "error" in response:
            raise AuthenticationError(
                response.get("message", "unknown server error"),
                self._api_endpoint,
            )


class Client(httpx.Client):
    cleandoc(
        """
        An http client, based on httpx.Client, that handles the authentication
        for Firebolt database.

        Authentication can be passed through auth keyword as a tuple or as a
        FireboltAuth instance

        httpx.Client:
        """
        + (httpx.Client.__doc__ or "")
    )

    def __init__(
        self,
        *args: Any,
        api_endpoint: str = DEFAULT_API_URL,
        auth: AuthTypes = None,
        **kwargs: Any,
    ):
        self._api_endpoint = api_endpoint
        super().__init__(*args, auth=auth, **kwargs)

    def _build_auth(self, auth: httpx._types.AuthTypes) -> typing.Optional[Auth]:
        if auth is None or isinstance(auth, Auth):
            return auth
        elif isinstance(auth, tuple):
            return Auth(
                username=str(auth[0]),
                password=str(auth[1]),
                api_endpoint=self._api_endpoint,
            )
        else:
            raise TypeError(f'Invalid "auth" argument: {auth!r}')

    @wraps(httpx.Client.send)
    def send(self, *args: Any, **kwargs: Any) -> httpx.Response:
        cleandoc(
            """
            Try to send request and if it fails with UNAUTHORIZED retry once
            with new token. Overrides httpx.Client.send
            """
        )
        resp = super().send(*args, **kwargs)
        if resp.status_code == httpx.codes.UNAUTHORIZED and isinstance(
            self._auth, Auth
        ):
            # get new token and try to send the request again
            self._auth.get_new_token()
            resp = super().send(*args, **kwargs)
        return resp

    @cached_property
    def account_id(self) -> str:
        return self.get(url="/iam/v2/account").json()["account"]["id"]
