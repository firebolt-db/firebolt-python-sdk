from typing import Any, Optional

from anyio._core._eventloop import get_asynclib
from async_property import async_cached_property  # type: ignore
from httpx import URL
from httpx import AsyncClient as HttpxAsyncClient
from httpx import Client as HttpxClient
from httpx import Request, Response
from httpx import codes as HttpxCodes
from httpx._types import AuthTypes

from firebolt.client.auth import Auth
from firebolt.client.auth.base import AuthRequest
from firebolt.client.constants import DEFAULT_API_URL
from firebolt.utils.exception import AccountNotFoundError
from firebolt.utils.urls import ACCOUNT_BY_NAME_URL, ACCOUNT_URL
from firebolt.utils.util import (
    cached_property,
    fix_url_schema,
    merge_urls,
    mixin_for,
)

# Explicitly import all available backend not get into
# anyio race condition during backend import
for backend in ("asyncio", "trio"):
    try:
        get_asynclib(backend)
    except ModuleNotFoundError:
        # Not all backends might be installed
        pass

FireboltClientMixinBase = mixin_for(HttpxClient)  # type: Any


class FireboltClientMixin(FireboltClientMixinBase):
    """HttpxAsyncClient mixin with Firebolt authentication functionality."""

    def __init__(
        self,
        *args: Any,
        account_name: Optional[str] = None,
        api_endpoint: str = DEFAULT_API_URL,
        auth: Optional[Auth] = None,
        **kwargs: Any,
    ):
        self.account_name = account_name
        self._api_endpoint = URL(fix_url_schema(api_endpoint))
        super().__init__(*args, auth=auth, **kwargs)

    def _build_auth(self, auth: Optional[AuthTypes]) -> Optional[Auth]:
        """Create Auth object based on auth provided.

        Overrides ``httpx.Client._build_auth``

        Args:
            auth (AuthTypes): Provided auth

        Returns:
            Optional[Auth]: Auth object

        Raises:
            TypeError: Auth argument has unsupported type
        """
        if auth is None or isinstance(auth, Auth):
            return auth
        raise TypeError(f'Invalid "auth" argument: {auth!r}')

    def _merge_auth_request(self, request: Request) -> Request:
        if isinstance(request, AuthRequest):
            request.url = merge_urls(self._api_endpoint, request.url)
            request._prepare(dict(request.headers))
        return request


class Client(FireboltClientMixin, HttpxClient):
    """An HTTP client, based on httpx.Client.

    Handles the authentication for Firebolt database.
    Authentication can be passed through auth keyword as a tuple or as a
    FireboltAuth instance
    """

    @cached_property
    def account_id(self) -> str:
        """User account ID.

        If account_name was provided during Client construction, returns its ID;
        gets default account otherwise.

        Returns:
            str: Account ID

        Raises:
            AccountNotFoundError: No account found with provided name
        """
        if self.account_name:
            response = self.get(
                url=ACCOUNT_BY_NAME_URL, params={"account_name": self.account_name}
            )
            if response.status_code == HttpxCodes.NOT_FOUND:
                raise AccountNotFoundError(self.account_name)
            # process all other status codes
            response.raise_for_status()
            return response.json()["account_id"]

        # account_name isn't set, use the default account.
        return self.get(url=ACCOUNT_URL).json()["account"]["id"]

    def _send_handling_redirects(
        self, request: Request, *args: Any, **kwargs: Any
    ) -> Response:
        return super()._send_handling_redirects(
            self._merge_auth_request(request), *args, **kwargs
        )


class AsyncClient(FireboltClientMixin, HttpxAsyncClient):
    """An HTTP client, based on httpx.AsyncClient.

    Asynchronously handles authentication for Firebolt database.
    Authentication can be passed through auth keyword as a tuple, or as a
    FireboltAuth instance.
    """

    @async_cached_property
    async def account_id(self) -> str:
        """User account id.

        If account_name was provided during AsyncClient construction, returns its ID;
        gets default account otherwise.

        Returns:
            str: Account ID

        Raises:
            AccountNotFoundError: No account found with provided name
        """
        if self.account_name:
            response = await self.get(
                url=ACCOUNT_BY_NAME_URL, params={"account_name": self.account_name}
            )
            if response.status_code == HttpxCodes.NOT_FOUND:
                raise AccountNotFoundError(self.account_name)
            # process all other status codes
            response.raise_for_status()
            return response.json()["account_id"]

        # account_name isn't set; use the default account.
        return (await self.get(url=ACCOUNT_URL)).json()["account"]["id"]

    async def _send_handling_redirects(
        self, request: Request, *args: Any, **kwargs: Any
    ) -> Response:
        return await super()._send_handling_redirects(
            self._merge_auth_request(request), *args, **kwargs
        )
