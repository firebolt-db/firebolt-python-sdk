from __future__ import annotations

from httpx import Timeout, codes

from firebolt.client.auth import Auth
from firebolt.client.client import AsyncClientV2
from firebolt.common.cache import _firebolt_cache
from firebolt.common.constants import DEFAULT_TIMEOUT_SECONDS
from firebolt.utils.exception import (
    AccountNotFoundOrNoAccessError,
    InterfaceError,
)
from firebolt.utils.urls import GATEWAY_HOST_BY_ACCOUNT_NAME
from firebolt.utils.util import EngineInfo, parse_url_and_params


async def _get_system_engine_url_and_params(
    auth: Auth,
    account_name: str,
    api_endpoint: str,
) -> EngineInfo:
    if result := _firebolt_cache.system_engine_cache.get([account_name, api_endpoint]):
        return result
    async with AsyncClientV2(
        auth=auth,
        base_url=api_endpoint,
        account_name=account_name,
        api_endpoint=api_endpoint,
        timeout=Timeout(DEFAULT_TIMEOUT_SECONDS),
    ) as client:
        url = GATEWAY_HOST_BY_ACCOUNT_NAME.format(account_name=account_name)
        response = await client.get(url=url)
        if response.status_code == codes.NOT_FOUND:
            raise AccountNotFoundOrNoAccessError(account_name)
        if response.status_code != codes.OK:
            raise InterfaceError(
                f"Unable to retrieve system engine endpoint {url}: "
                f"{response.status_code} {response.content.decode()}"
            )
        result = parse_url_and_params(response.json()["engineUrl"])
        _firebolt_cache.system_engine_cache.set(
            key=[account_name, api_endpoint], value=result
        )
        return result
