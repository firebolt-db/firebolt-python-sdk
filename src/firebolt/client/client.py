from json import JSONDecodeError
from typing import Any, Optional

from async_property import async_cached_property  # type: ignore
from httpx import URL
from httpx import AsyncClient as HttpxAsyncClient
from httpx import Client as HttpxClient
from httpx import HTTPStatusError, Request, RequestError, Response
from httpx import codes as HttpxCodes
from httpx._types import AuthTypes

from firebolt.client.auth import Auth
from firebolt.client.auth.base import AuthRequest
from firebolt.client.constants import DEFAULT_API_URL
from firebolt.utils.exception import (
    AccountNotFoundError,
    FireboltEngineError,
    InterfaceError,
)
from firebolt.utils.urls import (
    ACCOUNT_BY_NAME_URL,
    ACCOUNT_BY_NAME_URL_V1,
    ACCOUNT_ENGINE_ID_BY_NAME_URL,
    ACCOUNT_ENGINE_URL,
    ACCOUNT_ENGINE_URL_BY_DATABASE_NAME_V1,
    ACCOUNT_URL,
)
from firebolt.utils.util import (
    cached_property,
    fix_url_schema,
    get_auth_endpoint,
    merge_urls,
    mixin_for,
)

FireboltClientMixinBase = mixin_for(HttpxClient)  # type: Any


class FireboltClientMixin(FireboltClientMixinBase):
    """HttpxAsyncClient mixin with Firebolt authentication functionality."""

    def __init__(
        self,
        *args: Any,
        auth: Auth,
        account_name: Optional[str],
        api_endpoint: str = DEFAULT_API_URL,
        **kwargs: Any,
    ):
        self.account_name = account_name
        self._api_endpoint = URL(fix_url_schema(api_endpoint))
        self._auth_endpoint = get_auth_endpoint(self._api_endpoint)
        super().__init__(*args, auth=auth, **kwargs)

    def _build_auth(self, auth: Optional[AuthTypes]) -> Auth:
        """Create Auth object based on auth provided.

        Overrides ``httpx.Client._build_auth``

        Args:
            auth (AuthTypes): Provided auth

        Returns:
            Optional[Auth]: Auth object

        Raises:
            TypeError: Auth argument has unsupported type
        """
        if not (auth is None or isinstance(auth, Auth)):
            raise TypeError(f'Invalid "auth" argument: {auth!r}')
        assert auth is not None  # type check
        return auth

    def _merge_auth_request(self, request: Request) -> Request:
        if isinstance(request, AuthRequest):
            request.url = merge_urls(self._auth_endpoint, request.url)
            request._prepare(dict(request.headers))
        return request

    def _enforce_trailing_slash(self, url: URL) -> URL:
        """Don't automatically append trailing slach to a base url"""
        return url


class ClientV2(FireboltClientMixin, HttpxClient):
    """An HTTP client, based on httpx.Client.

    Handles the authentication for Firebolt database.
    Authentication can be passed through auth keyword as a tuple or as a
    FireboltAuth instance
    """

    def __init__(
        self,
        *args: Any,
        auth: Auth,
        account_name: str,
        api_endpoint: str = DEFAULT_API_URL,
        **kwargs: Any,
    ):
        super().__init__(
            *args,
            auth=auth,
            account_name=account_name,
            api_endpoint=api_endpoint,
            **kwargs,
        )

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
        response = self.get(
            url=self._api_endpoint.copy_with(
                path=ACCOUNT_BY_NAME_URL.format(account_name=self.account_name)
            )
        )
        if response.status_code == HttpxCodes.NOT_FOUND:
            assert self.account_name is not None
            raise AccountNotFoundError(self.account_name)
        # process all other status codes
        response.raise_for_status()
        return response.json()["id"]

    def _send_handling_redirects(
        self, request: Request, *args: Any, **kwargs: Any
    ) -> Response:
        return super()._send_handling_redirects(
            self._merge_auth_request(request), *args, **kwargs
        )


class ClientV1(FireboltClientMixin, HttpxClient):
    """An HTTP client, based on httpx.Client.

    Handles the authentication for Firebolt database.
    Authentication can be passed through auth keyword as a tuple or as a
    FireboltAuth instance
    """

    def __init__(
        self,
        *args: Any,
        auth: Auth,
        account_name: Optional[str],
        api_endpoint: str = DEFAULT_API_URL,
        **kwargs: Any,
    ):
        super().__init__(
            *args,
            auth=auth,
            account_name=account_name,
            api_endpoint=api_endpoint,
            **kwargs,
        )
        self._auth_endpoint = URL(fix_url_schema(api_endpoint))

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
                url=ACCOUNT_BY_NAME_URL_V1, params={"account_name": self.account_name}
            )  # TODO: url here might be incorrect
            if response.status_code == HttpxCodes.NOT_FOUND:
                raise AccountNotFoundError(self.account_name)
            # process all other status codes
            response.raise_for_status()
            return response.json()["account_id"]

        # account_name isn't set, use the default account.
        return self.get(url=ACCOUNT_URL).json()["account"]["id"]

    def _get_database_default_engine_url(
        self,
        database: str,
    ) -> str:
        try:
            account_id = self.account_id
            response = self.get(
                url=ACCOUNT_ENGINE_URL_BY_DATABASE_NAME_V1.format(
                    account_id=account_id
                ),
                params={"database_name": database},
            )
            response.raise_for_status()
            return response.json()["engine_url"]
        except (
            JSONDecodeError,
            RequestError,
            RuntimeError,
            HTTPStatusError,
            KeyError,
        ) as e:
            raise InterfaceError(f"Unable to retrieve default engine endpoint: {e}.")

    def _resolve_engine_url(self, engine_name: str) -> str:
        account_id = self.account_id
        url = ACCOUNT_ENGINE_ID_BY_NAME_URL.format(account_id=account_id)
        try:
            response = self.get(
                url=url,
                params={"engine_name": engine_name},
            )
            response.raise_for_status()
            engine_id = response.json()["engine_id"]["engine_id"]
            url = ACCOUNT_ENGINE_URL.format(account_id=account_id, engine_id=engine_id)
            response = self.get(url=url)
            response.raise_for_status()
            return response.json()["engine"]["endpoint"]
        except HTTPStatusError as e:
            # Engine error would be 404.
            if e.response.status_code != 404:
                raise InterfaceError(
                    f"Error {e.__class__.__name__}: Unable to retrieve engine "
                    f"endpoint {url}."
                )
            # Once this is point is reached we've already authenticated with
            # the backend so it's safe to assume the cause of the error is
            # missing engine.
            raise FireboltEngineError(f"Firebolt engine {engine_name} does not exist.")
        except (JSONDecodeError, RequestError, RuntimeError) as e:
            raise InterfaceError(
                f"Error {e.__class__.__name__}: "
                f"Unable to retrieve engine endpoint {url}."
            )

    def _send_handling_redirects(
        self, request: Request, *args: Any, **kwargs: Any
    ) -> Response:
        return super()._send_handling_redirects(
            self._merge_auth_request(request), *args, **kwargs
        )


class AsyncClientV2(FireboltClientMixin, HttpxAsyncClient):
    """An HTTP client, based on httpx.Client.

    Handles the authentication for Firebolt database.
    Authentication can be passed through auth keyword as a tuple or as a
    FireboltAuth instance
    """

    def __init__(
        self,
        *args: Any,
        auth: Auth,
        account_name: str,
        api_endpoint: str = DEFAULT_API_URL,
        **kwargs: Any,
    ):
        super().__init__(
            *args,
            auth=auth,
            account_name=account_name,
            api_endpoint=api_endpoint,
            **kwargs,
        )

    @async_cached_property
    async def account_id(self) -> str:
        """User account ID.

        If account_name was provided during Client construction, returns its ID;
        gets default account otherwise.

        Returns:
            str: Account ID

        Raises:
            AccountNotFoundError: No account found with provided name
        """
        response = await self.get(
            url=self._api_endpoint.copy_with(
                path=ACCOUNT_BY_NAME_URL.format(account_name=self.account_name)
            )
        )
        if response.status_code == HttpxCodes.NOT_FOUND:
            assert self.account_name is not None
            raise AccountNotFoundError(self.account_name)
        # process all other status codes
        response.raise_for_status()
        return response.json()["id"]

    async def _send_handling_redirects(
        self, request: Request, *args: Any, **kwargs: Any
    ) -> Response:
        return await super()._send_handling_redirects(
            self._merge_auth_request(request), *args, **kwargs
        )


class AsyncClientV1(FireboltClientMixin, HttpxAsyncClient):
    """An HTTP client, based on httpx.Client.

    Handles the authentication for Firebolt database.
    Authentication can be passed through auth keyword as a tuple or as a
    FireboltAuth instance
    """

    def __init__(
        self,
        *args: Any,
        auth: Auth,
        account_name: Optional[str],
        api_endpoint: str = DEFAULT_API_URL,
        **kwargs: Any,
    ):
        super().__init__(
            *args,
            auth=auth,
            account_name=account_name,
            api_endpoint=api_endpoint,
            **kwargs,
        )
        self._auth_endpoint = URL(fix_url_schema(api_endpoint))

    @async_cached_property
    async def account_id(self) -> str:
        """User account ID.

        If account_name was provided during Client construction, returns its ID;
        gets default account otherwise.

        Returns:
            str: Account ID

        Raises:
            AccountNotFoundError: No account found with provided name
        """
        if self.account_name:
            response = await self.get(
                url=ACCOUNT_BY_NAME_URL_V1, params={"account_name": self.account_name}
            )  # TODO: url here might be incorrect
            if response.status_code == HttpxCodes.NOT_FOUND:
                raise AccountNotFoundError(self.account_name)
            # process all other status codes
            response.raise_for_status()
            return response.json()["account_id"]

        # account_name isn't set, use the default account.
        return self.get(url=ACCOUNT_URL).json()["account"]["id"]

    async def _get_database_default_engine_url(
        self,
        database: str,
    ) -> str:
        try:
            account_id = await self.account_id
            response = await self.get(
                url=ACCOUNT_ENGINE_URL_BY_DATABASE_NAME_V1.format(
                    account_id=account_id
                ),
                params={"database_name": database},
            )
            response.raise_for_status()
            return response.json()["engine_url"]
        except (
            JSONDecodeError,
            RequestError,
            RuntimeError,
            HTTPStatusError,
            KeyError,
        ) as e:
            raise InterfaceError(f"Unable to retrieve default engine endpoint: {e}.")

    async def _resolve_engine_url(self, engine_name: str) -> str:
        account_id = await self.account_id
        url = ACCOUNT_ENGINE_ID_BY_NAME_URL.format(account_id=account_id)
        try:
            response = await self.get(
                url=url,
                params={"engine_name": engine_name},
            )
            response.raise_for_status()
            engine_id = response.json()["engine_id"]["engine_id"]
            url = ACCOUNT_ENGINE_URL.format(account_id=account_id, engine_id=engine_id)
            response = await self.get(url=url)
            response.raise_for_status()
            return response.json()["engine"]["endpoint"]
        except HTTPStatusError as e:
            # Engine error would be 404.
            if e.response.status_code != 404:
                raise InterfaceError(
                    f"Error {e.__class__.__name__}: Unable to retrieve engine "
                    f"endpoint {url}."
                )
            # Once this is point is reached we've already authenticated with
            # the backend so it's safe to assume the cause of the error is
            # missing engine.
            raise FireboltEngineError(f"Firebolt engine {engine_name} does not exist.")
        except (JSONDecodeError, RequestError, RuntimeError) as e:
            raise InterfaceError(
                f"Error {e.__class__.__name__}: "
                f"Unable to retrieve engine endpoint {url}."
            )

    async def _send_handling_redirects(
        self, request: Request, *args: Any, **kwargs: Any
    ) -> Response:
        return await super()._send_handling_redirects(
            self._merge_auth_request(request), *args, **kwargs
        )
