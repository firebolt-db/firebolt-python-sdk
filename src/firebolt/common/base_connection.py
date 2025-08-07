from collections import namedtuple
from typing import Any, Dict, List, Optional, Tuple, Type

from firebolt.client.auth.base import Auth
from firebolt.common._types import ColType
from firebolt.utils.cache import (
    ConnectionInfo,
    EngineInfo,
    SecureCacheKey,
    _firebolt_cache,
)
from firebolt.utils.exception import ConnectionClosedError, FireboltError
from firebolt.utils.usage_tracker import (
    get_cache_tracking_params,
    get_user_agent_header,
)

ASYNC_QUERY_STATUS_RUNNING = "RUNNING"
ASYNC_QUERY_STATUS_SUCCESSFUL = "ENDED_SUCCESSFULLY"
ASYNC_QUERY_STATUS_REQUEST = "CALL fb_GetAsyncStatus(?)"
ASYNC_QUERY_CANCEL = "CANCEL QUERY WHERE query_id=?"

AsyncQueryInfo = namedtuple(
    "AsyncQueryInfo",
    [
        "account_name",
        "user_name",
        "submitted_time",
        "start_time",
        "end_time",
        "status",
        "request_id",
        "query_id",
        "error_message",
        "scanned_bytes",
        "scanned_rows",
        "retries",
    ],
)


def _parse_async_query_info_results(
    result: List[List[ColType]], columns_names: List[str]
) -> List[AsyncQueryInfo]:
    async_query_infos = []
    for row in result:
        result_dict = dict(zip(columns_names, row))

        if not result_dict.get("status") or not result_dict.get("query_id"):
            raise FireboltError(
                "Something went wrong - async query status request returned "
                "unexpected result with status and/or query id missing. "
                "Rerun the command and reach out to Firebolt support if "
                "the issue persists."
            )

        # Only pass the expected keys to AsyncQueryInfo
        filtered_result_dict = {
            k: v for k, v in result_dict.items() if k in AsyncQueryInfo._fields
        }

        async_query_infos.append(AsyncQueryInfo(**filtered_result_dict))
    return async_query_infos


class BaseConnection:
    def __init__(self, cursor_type: Type) -> None:
        self.cursor_type = cursor_type
        self._cursors: List[Any] = []
        self._is_closed = False

    def _remove_cursor(self, cursor: Any) -> None:
        # This way it's atomic
        try:
            self._cursors.remove(cursor)
        except ValueError:
            pass

    @property
    def closed(self) -> bool:
        """`True` if connection is closed; `False` otherwise."""
        return self._is_closed

    def commit(self) -> None:
        """Does nothing since Firebolt doesn't have transactions."""

        if self.closed:
            raise ConnectionClosedError("Unable to commit: Connection closed.")


def get_cached_system_engine_info(
    auth: Auth,
    account_name: str,
    disable_cache: bool = False,
) -> Tuple[SecureCacheKey, Optional[EngineInfo]]:
    """
    Common cache retrieval logic for system engine info.

    Returns:
        tuple: (cache_key, cached_engine_info_or_none)
    """
    cache_key = SecureCacheKey([auth.principal, auth.secret, account_name], auth.secret)

    if disable_cache:
        return cache_key, None

    cache = _firebolt_cache.get(cache_key)
    cached_result = cache.system_engine if cache else None

    return cache_key, cached_result


def set_cached_system_engine_info(
    cache_key: SecureCacheKey,
    connection_id: str,
    url: str,
    params: dict,
    disable_cache: bool = False,
) -> EngineInfo:
    """
    Common cache setting logic for system engine info.

    Returns:
        EngineInfo: The engine info that was cached (or created)
    """

    engine_info = EngineInfo(url=url, params=params)

    if not disable_cache:
        cache = _firebolt_cache.get(cache_key)
        if not cache:
            cache = ConnectionInfo(id=connection_id)
        cache.system_engine = engine_info
        _firebolt_cache.set(cache_key, cache)

    return engine_info


def get_user_agent_for_connection(
    auth: Auth,
    connection_id: str,
    account_name: Optional[str] = None,
    additional_parameters: Dict[str, Any] = {},
    disable_cache: bool = False,
) -> str:
    """
    Get the user agent string for the Firebolt connection.

    Returns:
        str: The user agent string.
    """
    user_drivers = additional_parameters.get("user_drivers", [])
    user_clients = additional_parameters.get("user_clients", [])
    ua_parameters = []
    if not disable_cache:
        cache_key = SecureCacheKey(
            [auth.principal, auth.secret, account_name], auth.secret
        )
        ua_parameters = get_cache_tracking_params(cache_key, connection_id)
    user_agent_header = get_user_agent_header(user_drivers, user_clients, ua_parameters)
    return user_agent_header
