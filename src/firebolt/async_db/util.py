from __future__ import annotations

from httpx import Timeout, codes

from firebolt.client.auth import Auth
from firebolt.client.client import AsyncClientV2
from firebolt.common.settings import DEFAULT_TIMEOUT_SECONDS
from firebolt.utils.exception import (
    AccountNotFoundOrNoAccessError,
    InterfaceError,
)
from firebolt.utils.urls import GATEWAY_HOST_BY_ACCOUNT_NAME

ENGINE_STATUS_RUNNING = "Running"


async def _get_system_engine_url(
    auth: Auth,
    account_name: str,
    api_endpoint: str,
) -> str:
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
        return response.json()["engineUrl"]
