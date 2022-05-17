from __future__ import annotations

from functools import wraps
from threading import Lock
from typing import Any, Dict, Generator, List, Optional, Sequence, Tuple

from readerwriterlock.rwlock import RWLockWrite

from firebolt.async_db._types import ColType
from firebolt.async_db.cursor import BaseCursor as AsyncBaseCursor
from firebolt.async_db.cursor import (
    ParameterType,
    check_not_closed,
    check_query_executed,
)
from firebolt.common.util import AsyncJobThread, async_to_sync


class Cursor(AsyncBaseCursor):
    """
    Class, responsible for executing queries to Firebolt Database.
    Should not be created directly,
    use :py:func:`connection.cursor <firebolt.async_db.connection.Connection>`

    Args:
        description: Information about a single result row
        rowcount: The number of rows produced by last query
        closed: True if connection is closed, False otherwise
        arraysize: Read/Write, specifies the number of rows to fetch at a time
            with the :py:func:`fetchmany` method
    """

    __slots__ = AsyncBaseCursor.__slots__ + (
        "_query_lock",
        "_idx_lock",
        "_async_job_thread",
    )

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._query_lock = RWLockWrite()
        self._idx_lock = Lock()
        self._async_job_thread: AsyncJobThread = kwargs.pop("async_job_thread")
        super().__init__(*args, **kwargs)

    @wraps(AsyncBaseCursor.execute)
    def execute(
        self,
        query: str,
        parameters: Optional[Sequence[ParameterType]] = None,
        set_parameters: Optional[Dict] = None,
        skip_parsing: bool = False,
    ) -> int:
        with self._query_lock.gen_wlock():
            return async_to_sync(super().execute, self._async_job_thread)(
                query, parameters, set_parameters, skip_parsing
            )

    @wraps(AsyncBaseCursor.executemany)
    def executemany(
        self, query: str, parameters_seq: Sequence[Sequence[ParameterType]]
    ) -> int:
        with self._query_lock.gen_wlock():
            return async_to_sync(super().executemany, self._async_job_thread)(
                query, parameters_seq
            )

    @wraps(AsyncBaseCursor._get_next_range)
    def _get_next_range(self, size: int) -> Tuple[int, int]:
        with self._idx_lock:
            return super()._get_next_range(size)

    @wraps(AsyncBaseCursor.fetchone)
    def fetchone(self) -> Optional[List[ColType]]:
        with self._query_lock.gen_rlock():
            return super().fetchone()

    @wraps(AsyncBaseCursor.fetchmany)
    def fetchmany(self, size: Optional[int] = None) -> List[List[ColType]]:
        with self._query_lock.gen_rlock():
            return super().fetchmany(size)

    @wraps(AsyncBaseCursor.fetchall)
    def fetchall(self) -> List[List[ColType]]:
        with self._query_lock.gen_rlock():
            return super().fetchall()

    @wraps(AsyncBaseCursor.nextset)
    def nextset(self) -> None:
        with self._query_lock.gen_rlock(), self._idx_lock:
            return super().nextset()

    # Iteration support
    @check_not_closed
    @check_query_executed
    def __iter__(self) -> Generator[List[ColType], None, None]:
        while True:
            row = self.fetchone()
            if row is None:
                return
            yield row
