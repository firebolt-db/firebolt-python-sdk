import typing
from inspect import cleandoc
from typing import Any

import httpx
from httpx._types import AuthTypes

from firebolt.client.auth import Auth
from firebolt.client.constants import DEFAULT_API_URL
from firebolt.common.utils import cached_property


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

    @cached_property
    def account_id(self) -> str:
        return self.get(url="/iam/v2/account").json()["account"]["id"]
