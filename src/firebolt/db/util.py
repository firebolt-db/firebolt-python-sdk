from __future__ import annotations

from httpx import Timeout, codes

from firebolt.client import ClientV2
from firebolt.client.auth import Auth
from firebolt.common.constants import DEFAULT_TIMEOUT_SECONDS
from firebolt.utils.cache import ConnectionInfo, EngineInfo, _firebolt_cache
from firebolt.utils.exception import (
    AccountNotFoundOrNoAccessError,
    InterfaceError,
)
from firebolt.utils.urls import GATEWAY_HOST_BY_ACCOUNT_NAME
from firebolt.utils.util import parse_url_and_params


def _get_system_engine_url_and_params(
    auth: Auth,
    account_name: str,
    api_endpoint: str,
    connection_id: str,
) -> EngineInfo:
    cache = _firebolt_cache.get([account_name, api_endpoint])
    if cache and (result := cache.system_engine):
        return result
    with ClientV2(
        auth=auth,
        base_url=api_endpoint,
        account_name=account_name,
        api_endpoint=api_endpoint,
        timeout=Timeout(DEFAULT_TIMEOUT_SECONDS),
    ) as client:
        url = GATEWAY_HOST_BY_ACCOUNT_NAME.format(account_name=account_name)
        response = client.get(url=url)
        if response.status_code == codes.NOT_FOUND:
            raise AccountNotFoundOrNoAccessError(account_name)
        if response.status_code != codes.OK:
            raise InterfaceError(
                f"Unable to retrieve system engine endpoint {url}: "
                f"{response.status_code} {response.content.decode()}"
            )
        url, params = parse_url_and_params(response.json()["engineUrl"])
        if not cache:
            cache = ConnectionInfo(id=connection_id)
            _firebolt_cache.set([account_name, api_endpoint], cache)
        cache.system_engine = EngineInfo(url=url, params=params)
        return cache.system_engine
