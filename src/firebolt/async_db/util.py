from __future__ import annotations

from typing import Dict, Tuple

from httpx import Timeout, codes

from firebolt.client.auth import Auth
from firebolt.client.client import AsyncClientV2
from firebolt.common.constants import DEFAULT_TIMEOUT_SECONDS
from firebolt.utils.exception import (
    AccountNotFoundOrNoAccessError,
    InterfaceError,
)
from firebolt.utils.urls import GATEWAY_HOST_BY_ACCOUNT_NAME
from firebolt.utils.util import parse_url_and_params

_firebolt_system_engine_cache: Dict[str, Tuple[str, Dict[str, str]]] = {}


async def _get_system_engine_url_and_params(
    auth: Auth,
    account_name: str,
    api_endpoint: str,
) -> Tuple[str, Dict[str, str]]:
    cache_key = f"{account_name}-{api_endpoint}"
    if cache_key in _firebolt_system_engine_cache:
        return _firebolt_system_engine_cache[cache_key]
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
        _firebolt_system_engine_cache[cache_key] = result
        return result
