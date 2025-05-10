from collections import namedtuple
from typing import Any, List, Type

from firebolt.common._types import ColType
from firebolt.utils.exception import ConnectionClosedError, FireboltError

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
