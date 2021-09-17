import time
import typing
from functools import wraps
from inspect import cleandoc
from json import JSONDecodeError

import httpx
import httpx._types

DEFAULT_API_URL: str = "api.app.firebolt.io"
API_REQUEST_TIMEOUT_SECONDS: typing.Optional[int] = 30
_REQUEST_ERRORS: typing.Tuple[type[Exception], ...] = (
    httpx.HTTPError,
    httpx.InvalidURL,
    httpx.CookieConflict,
    httpx.StreamError,
    JSONDecodeError,
    KeyError,
    ValueError,
)


class AuthenticationError(Exception):
    cleandoc(
        """
        Firebolt authentication error. Stores error cause and authnetication endpoint
        """
    )

    def __init__(self, cause: str, api_endpoint: str):
        self.cause = cause
        self.api_endpoint = api_endpoint

    def __str__(self) -> str:
        return f"Failed to authenticate at {self.api_endpoint}: {self.cause}"


class FireboltAuth(httpx.Auth):
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
        self._api_endpoint = api_endpoint
        self._token: typing.Optional[str] = None
        self._expires: typing.Optional[int] = None

    @property
    def token(self) -> typing.Optional[str]:
        if not self._token or self.expired:
            self.get_new_token()
        return self._token

    @property
    def expired(self) -> typing.Optional[int]:
        return self._expires is not None and self._expires <= int(time.time())

    def get_new_token(self) -> None:
        """Get new token using username and password"""
        try:
            response = httpx.post(
                f"https://{self._api_endpoint}/auth/v1/login",
                headers={"Content-Type": "application/json;charset=UTF-8"},
                json={"username": self.username, "password": self.password},
                timeout=API_REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()

            parsed = response.json()
            self._check_response_error(parsed)

            self._token = parsed["access_token"]
            self._expires = int(time.time()) + int(parsed["expiry"])
        except _REQUEST_ERRORS as e:
            raise AuthenticationError(repr(e), self._api_endpoint)

    def auth_flow(
        self, request: httpx.Request
    ) -> typing.Generator[httpx.Request, httpx.Response, None]:
        request.headers["Authorization"] = f"Bearer {self.token}"
        yield request

    def _check_response_error(self, response: typing.Dict) -> None:
        if "error" in response:
            raise AuthenticationError(
                response.get("message", "unknown server error"),
                self._api_endpoint,
            )


class FireboltClient(httpx.Client):
    cleandoc(
        """
        An http client, based on httpx.Client, that handles the authentificaiton
        for Firebolt database.

        Authentification can be passed through auth keyword as a tuple or as a
        FireboltAuth instance

        httpx.Client:
        """
        + ("" if httpx.Client.__doc__ is None else httpx.Client.__doc__)
    )

    def __init__(
        self,
        *args: typing.Any,
        api_endpoint: str = DEFAULT_API_URL,
        auth: httpx._types.AuthTypes = None,
        **kwargs: typing.Any,
    ):
        self._api_endpoint = api_endpoint
        super().__init__(*args, auth=auth, **kwargs)

    def _build_auth(
        self, auth: httpx._types.AuthTypes
    ) -> typing.Optional[FireboltAuth]:
        if auth is None or isinstance(auth, FireboltAuth):
            return auth
        elif isinstance(auth, tuple):
            return FireboltAuth(
                username=str(auth[0]),
                password=str(auth[1]),
                api_endpoint=self._api_endpoint,
            )
        else:
            raise TypeError(f'Invalid "auth" argument: {auth!r}')

    @wraps(httpx.Client.send)
    def send(self, *args: typing.Any, **kwargs: typing.Any) -> httpx.Response:
        resp = super().send(*args, **kwargs)
        if resp.status_code == httpx.codes.UNAUTHORIZED and isinstance(
            self._auth, FireboltAuth
        ):
            # get new token and try to send the request again
            self._auth.get_new_token()
            resp = super().send(*args, **kwargs)
        return resp
