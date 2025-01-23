from typing import Any, Callable, List, Type

# from firebolt.common.base_cursor import BaseCursor
from firebolt.async_db.cursor import CursorV2
from firebolt.utils.exception import ConnectionClosedError, FireboltError

ASYNC_QUERY_STATUS_RUNNING = "RUNNING"
ASYNC_QUERY_STATUS_SUCCESSFUL = "ENDED_SUCCESSFULLY"
ASYNC_QUERY_STATUS_REQUEST = "CALL fb_GetAsyncStatus('{token}')"


def ensure_v2(func: Callable) -> Callable:
    """Decorator to ensure that the method is only called for CursorV2."""

    def wrapper(self: BaseConnection, *args: Any, **kwargs: Any) -> Any:
        if self.cursor_type != CursorV2:
            raise FireboltError("This method is only supported for CursorV2.")
        return func(self, *args, **kwargs)

    return wrapper


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
