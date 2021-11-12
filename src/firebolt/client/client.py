from inspect import cleandoc
from typing import Any, Optional

from async_property import async_cached_property  # type: ignore
from httpx import AsyncClient as HttpxAsyncClient
from httpx import Client as HttpxClient
from httpx import _types
from httpx._types import AuthTypes

from firebolt.client.auth import Auth
from firebolt.client.constants import DEFAULT_API_URL
from firebolt.common.urls import ACCOUNT_URL
from firebolt.common.util import cached_property, fix_url_schema, mixin_for

FireboltClientMixinBase = mixin_for(HttpxClient)  # type: Any


class FireboltClientMixin(FireboltClientMixinBase):
    def __init__(
        self,
        *args: Any,
        api_endpoint: str = DEFAULT_API_URL,
        auth: AuthTypes = None,
        **kwargs: Any,
    ):
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
    cleandoc(
        """
        An http client, based on httpx.Client, that handles the authentication
        for Firebolt database.

        Authentication can be passed through auth keyword as a tuple or as a
        FireboltAuth instance

        httpx.Client:
        """
        + (HttpxClient.__doc__ or "")
    )

    @cached_property
    def account_id(self) -> str:
        return self.get(url=ACCOUNT_URL).json()["account"]["id"]


class AsyncClient(FireboltClientMixin, HttpxAsyncClient):
    cleandoc(
        """
        An http client, based on httpx.AsyncClient, that asyncronously handles
        authentication for Firebolt database.

        Authentication can be passed through auth keyword as a tuple or as a
        FireboltAuth instance

        httpx.AsyncClient:
        """
        + (HttpxAsyncClient.__doc__ or "")
    )

    @async_cached_property
    async def account_id(self) -> str:
        return (await self.get(url=ACCOUNT_URL)).json()["account"]["id"]
