from typing import Any, Optional

from async_property import async_cached_property  # type: ignore
from httpx import AsyncClient as HttpxAsyncClient
from httpx import Client as HttpxClient
from httpx import _types
from httpx import codes as HttpxCodes
from httpx._types import AuthTypes

from firebolt.client.auth import Auth
from firebolt.client.constants import DEFAULT_API_URL
from firebolt.common.exception import AccountNotFoundError
from firebolt.common.urls import ACCOUNT_BY_NAME_URL, ACCOUNT_URL
from firebolt.common.util import cached_property, fix_url_schema, mixin_for

FireboltClientMixinBase = mixin_for(HttpxClient)  # type: Any


class FireboltClientMixin(FireboltClientMixinBase):
    def __init__(
        self,
        *args: Any,
        account_name: Optional[str] = None,
        api_endpoint: str = DEFAULT_API_URL,
        auth: AuthTypes = None,
        **kwargs: Any,
    ):
        self.account_name = account_name
        self._api_endpoint = fix_url_schema(api_endpoint)
        super().__init__(*args, auth=auth, **kwargs)

    def _build_auth(self, auth: _types.AuthTypes) -> Optional[Auth]:
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


class Client(FireboltClientMixin, HttpxClient):

    """
    An http client, based on httpx.Client, that handles the authentication
    for Firebolt database.

    Authentication can be passed through auth keyword as a tuple or as a
    FireboltAuth instance

    httpx.Client:

    + (HttpxClient.__doc__ or "")
    """

    @cached_property
    def account_id(self) -> str:
        if self.account_name:
            response = self.get(
                url=ACCOUNT_BY_NAME_URL, params={"account_name": self.account_name}
            )
            if response.status_code == HttpxCodes.NOT_FOUND:
                raise AccountNotFoundError(self.account_name)
            # process all other status codes
            response.raise_for_status()
            return response.json()["account_id"]
        else:  # account_name isn't set, use the default account.
            return self.get(url=ACCOUNT_URL).json()["account"]["id"]


class AsyncClient(FireboltClientMixin, HttpxAsyncClient):
    """
    An http client, based on httpx.AsyncClient, that asyncronously handles
    authentication for Firebolt database.

    Authentication can be passed through auth keyword as a tuple or as a
    FireboltAuth instance

    httpx.AsyncClient:

    + (HttpxAsyncClient.__doc__ or "")
    """

    @async_cached_property
    async def account_id(self) -> str:
        if self.account_name:
            response = await self.get(
                url=ACCOUNT_BY_NAME_URL, params={"account_name": self.account_name}
            )
            if response.status_code == HttpxCodes.NOT_FOUND:
                raise AccountNotFoundError(self.account_name)
            # process all other status codes
            response.raise_for_status()
            return response.json()["account_id"]
        else:  # account_name isn't set; use the default account.
            return (await self.get(url=ACCOUNT_URL)).json()["account"]["id"]
