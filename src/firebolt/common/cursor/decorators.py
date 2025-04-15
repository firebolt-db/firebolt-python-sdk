from __future__ import annotations

from functools import wraps
from typing import TYPE_CHECKING, Any, Callable

from firebolt.common.constants import CursorState
from firebolt.utils.exception import (
    CursorClosedError,
    MethodNotAllowedInAsyncError,
    QueryNotRunError,
)

if TYPE_CHECKING:
    from firebolt.common.cursor.base_cursor import BaseCursor


def check_not_closed(func: Callable) -> Callable:
    """(Decorator) ensure cursor is not closed before calling method."""

    @wraps(func)
    def inner(self: BaseCursor, *args: Any, **kwargs: Any) -> Any:
        if self.closed:
            raise CursorClosedError(method_name=func.__name__)
        return func(self, *args, **kwargs)

    return inner


def check_query_executed(func: Callable) -> Callable:
    """
    (Decorator) ensure that some query has been executed before
    calling cursor method.
    """

    @wraps(func)
    def inner(self: BaseCursor, *args: Any, **kwargs: Any) -> Any:
        if self._state == CursorState.NONE or self._row_set is None:
            raise QueryNotRunError(method_name=func.__name__)
        if self._query_token:
            # query_token is set only for async queries
            raise MethodNotAllowedInAsyncError(method_name=func.__name__)
        return func(self, *args, **kwargs)

    return inner


def async_not_allowed(func: Callable) -> Callable:
    """
    (Decorator) ensure that fetch methods are not called on async queries.
    """

    @wraps(func)
    def inner(self: BaseCursor, *args: Any, **kwargs: Any) -> Any:
        if self._query_token:
            # query_token is set only for async queries
            raise MethodNotAllowedInAsyncError(method_name=func.__name__)
        return func(self, *args, **kwargs)

    return inner
