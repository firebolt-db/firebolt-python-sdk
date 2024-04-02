from __future__ import annotations

from typing import Dict, Tuple
from urllib.parse import parse_qs, urlparse

from httpx import Timeout, codes

from firebolt.client import ClientV2
from firebolt.client.auth import Auth
from firebolt.common.constants import DEFAULT_TIMEOUT_SECONDS
from firebolt.utils.exception import (
    AccountNotFoundOrNoAccessError,
    InterfaceError,
)
from firebolt.utils.urls import GATEWAY_HOST_BY_ACCOUNT_NAME


def _get_system_engine_url_and_params(
    auth: Auth,
    account_name: str,
    api_endpoint: str,
) -> Tuple[str, Dict[str, str]]:
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
        raw_url = urlparse(response.json()["engineUrl"])
        url = raw_url.scheme + "://" + raw_url.netloc + raw_url.path
        query_params = parse_qs(raw_url.query)
        # parse_qs returns a dictionary with values as lists.
        # We want the last value in the list.
        query_params_dict = {}
        for key, values in query_params.items():
            # Multiple values for the same key are not expected
            if len(values) > 1:
                raise ValueError(f"Multiple values found for key '{key}'")
            query_params_dict[key] = values[-1]
        return url, query_params_dict
