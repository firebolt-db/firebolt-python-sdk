import typing
from inspect import cleandoc
from typing import Any

from httpx import AsyncClient as xAsyncClient
from httpx import Client as xClient
from httpx import _types
from httpx._types import AuthTypes

from firebolt.client.auth import Auth
from firebolt.client.constants import DEFAULT_API_URL
from firebolt.common.util import cached_property, mixin_for

FireboltClientMixinBase = mixin_for(xClient)  # type: Any


class FireboltClientMixin(FireboltClientMixinBase):
    def __init__(
        self,
        *args: Any,
        api_endpoint: str = DEFAULT_API_URL,
        auth: AuthTypes = None,
        **kwargs: Any,
    ):
        self._api_endpoint = api_endpoint
        super().__init__(*args, auth=auth, **kwargs)

    def _build_auth(self, auth: _types.AuthTypes) -> typing.Optional[Auth]:
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

    @cached_property
    def account_id(self) -> str:
        return self.get(url="/iam/v2/account").json()["account"]["id"]


class Client(FireboltClientMixin, xClient):
    cleandoc(
        """
        An http client, based on httpx.Client, that handles the authentication
        for Firebolt database.

        Authentication can be passed through auth keyword as a tuple or as a
        FireboltAuth instance

        httpx.Client:
        """
        + (xClient.__doc__ or "")
    )


class AsyncClient(FireboltClientMixin, xAsyncClient):
    cleandoc(
        """
        An http client, based on httpx.AsyncClient, that asyncronously handles
        authentication for Firebolt database.

        Authentication can be passed through auth keyword as a tuple or as a
        FireboltAuth instance

        httpx.AsyncClient:
        """
        + (xAsyncClient.__doc__ or "")
    )
