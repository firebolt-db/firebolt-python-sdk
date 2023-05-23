from __future__ import annotations

import logging
import re
import time
from threading import Lock
from types import TracebackType
from typing import (
    TYPE_CHECKING,
    Any,
    Generator,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from httpx import Client as HttpxClient
from httpx import Response, codes
from readerwriterlock.rwlock import RWLockWrite

from firebolt.common._types import (
    ColType,
    Column,
    ParameterType,
    RawColType,
    SetParameter,
    split_format_sql,
)
from firebolt.common.base_cursor import (
    JSON_OUTPUT_FORMAT,
    BaseCursor,
    CursorState,
    QueryStatus,
    Statistics,
    check_not_closed,
    check_query_executed,
)
from firebolt.db.util import is_db_available, is_engine_running
from firebolt.utils.exception import (
    AsyncExecutionUnavailableError,
    EngineNotRunningError,
    FireboltDatabaseError,
    OperationalError,
    ProgrammingError,
)

if TYPE_CHECKING:
    from firebolt.db.connection import Connection

logger = logging.getLogger(__name__)


class Cursor(BaseCursor):
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

    __slots__ = BaseCursor.__slots__ + (
        "_query_lock",
        "_idx_lock",
    )

    def __init__(
        self, *args: Any, client: HttpxClient, connection: Connection, **kwargs: Any
    ) -> None:
        super().__init__(*args, **kwargs)
        self._query_lock = RWLockWrite()
        self._idx_lock = Lock()
        self._client = client
        self.connection = connection

    def _raise_if_error(self, resp: Response) -> None:
        """Raise a proper error if any"""
        if resp.status_code == codes.INTERNAL_SERVER_ERROR:
            raise OperationalError(
                f"Error executing query:\n{resp.read().decode('utf-8')}"
            )
        if resp.status_code == codes.FORBIDDEN:
            if not is_db_available(self.connection, self.connection.database):
                raise FireboltDatabaseError(
                    f"Database {self.connection.database} does not exist"
                )
            raise ProgrammingError(resp.read().decode("utf-8"))
        if (
            resp.status_code == codes.SERVICE_UNAVAILABLE
            or resp.status_code == codes.NOT_FOUND
        ):
            if not is_engine_running(self.connection, self.connection.engine_url):
                raise EngineNotRunningError(
                    f"Firebolt engine {self.connection.engine_url} "
                    "needs to be running to run queries against it."
                )
        resp.raise_for_status()

    def _api_request(
        self,
        query: Optional[str] = "",
        parameters: Optional[dict[str, Any]] = {},
        path: Optional[str] = "",
        use_set_parameters: Optional[bool] = True,
    ) -> Response:
        """
        Query API, return Response object.

        Args:
            query (str): SQL query
            parameters (Optional[Sequence[ParameterType]]): A sequence of substitution
                parameters. Used to replace '?' placeholders inside a query with
                actual values. Note: In order to "output_format" dict value, it
                    must be an empty string. If no value not specified,
                    JSON_OUTPUT_FORMAT will be used.
            path (str): endpoint suffix, for example "cancel" or "status"
            use_set_parameters: Optional[bool]: Some queries will fail if additional
                set parameters are sent. Setting this to False will allow
                self._set_parameters to be ignored.
        """
        if use_set_parameters:
            parameters = {**(self._set_parameters or {}), **(parameters or {})}
        return self._client.request(
            url=f"/{path}",
            method="POST",
            params={
                "database": self.connection.database,
                **(parameters or dict()),
            },
            content=query,
        )

    def _validate_set_parameter(self, parameter: SetParameter) -> None:
        """Validate parameter by executing simple query with it."""
        if parameter.name == "async_execution":
            raise AsyncExecutionUnavailableError(
                "It is not possible to set async_execution using a SET command. "
                "Instead, pass it as an argument to the execute() or "
                "executemany() function."
            )
        resp = self._api_request("select 1", {parameter.name: parameter.value})
        # Handle invalid set parameter
        if resp.status_code == codes.BAD_REQUEST:
            raise OperationalError(resp.text)
        self._raise_if_error(resp)

        # set parameter passed validation
        self._set_parameters[parameter.name] = parameter.value

    def _do_execute(
        self,
        raw_query: str,
        parameters: Sequence[Sequence[ParameterType]],
        skip_parsing: bool = False,
        async_execution: Optional[bool] = False,
    ) -> None:
        self._reset()
        # Allow users to manually skip parsing for performance improvement.
        queries: List[Union[SetParameter, str]] = (
            [raw_query] if skip_parsing else split_format_sql(raw_query, parameters)
        )
        try:
            for query in queries:

                start_time = time.time()
                # Our CREATE EXTERNAL TABLE queries currently require credentials,
                # so we will skip logging those queries.
                # https://docs.firebolt.io/sql-reference/commands/create-external-table.html
                if isinstance(query, SetParameter) or not re.search(
                    "aws_key_id|credentials", query, flags=re.IGNORECASE
                ):
                    logger.debug(f"Running query: {query}")

                # Define type for mypy
                row_set: Tuple[
                    int,
                    Optional[List[Column]],
                    Optional[Statistics],
                    Optional[List[List[RawColType]]],
                ] = (-1, None, None, None)
                if isinstance(query, SetParameter):
                    self._validate_set_parameter(query)
                elif async_execution:
                    self._validate_server_side_async_settings(
                        parameters,
                        queries,
                        skip_parsing,
                        async_execution,
                    )
                    response = self._api_request(
                        query,
                        {
                            "async_execution": 1,
                            "advanced_mode": 1,
                            "output_format": JSON_OUTPUT_FORMAT,
                        },
                    )
                    self._raise_if_error(response)
                    if response.headers.get("content-length", "") == "0":
                        raise OperationalError("No response to asynchronous query.")
                    resp = response.json()
                    if "query_id" not in resp or resp["query_id"] == "":
                        raise OperationalError(
                            "Invalid response to asynchronous query: missing query_id."
                        )
                    self._query_id = resp["query_id"]
                else:
                    resp = self._api_request(
                        query, {"output_format": JSON_OUTPUT_FORMAT}
                    )
                    self._raise_if_error(resp)
                    row_set = self._row_set_from_response(resp)

                self._append_row_set(row_set)

                logger.info(
                    f"Query fetched {self.rowcount} rows in"
                    f" {time.time() - start_time} seconds."
                )

            self._state = CursorState.DONE

        except Exception:
            self._state = CursorState.ERROR
            raise

    @check_not_closed
    def execute(
        self,
        query: str,
        parameters: Optional[Sequence[ParameterType]] = None,
        skip_parsing: bool = False,
        async_execution: Optional[bool] = False,
    ) -> Union[int, str]:
        """Prepare and execute a database query.

        Supported features:
            Parameterized queries: placeholder characters ('?') are substituted
                with values provided in `parameters`. Values are formatted to
                be properly recognized by database and to exclude SQL injection.
            Multi-statement queries: multiple statements, provided in a single query
                and separated by semicolon, are executed separatelly and sequentially.
                To switch to next statement result, `nextset` method should be used.
            SET statements: to provide additional query execution parameters, execute
                `SET param=value` statement before it. All parameters are stored in
                cursor object until it's closed. They can also be removed with
                `flush_parameters` method call.

        Args:
            query (str): SQL query to execute
            parameters (Optional[Sequence[ParameterType]]): A sequence of substitution
                parameters. Used to replace '?' placeholders inside a query with
                actual values
            skip_parsing (bool): Flag to disable query parsing. This will
                disable parameterized, multi-statement and SET queries,
                while improving performance
            async_execution (bool): flag to determine if query should be asynchronous

        Returns:
            int: Query row count.
        """
        params_list = [parameters] if parameters else []
        self._do_execute(query, params_list, skip_parsing, async_execution)
        return self.query_id if async_execution else self.rowcount

    @check_not_closed
    def executemany(
        self,
        query: str,
        parameters_seq: Sequence[Sequence[ParameterType]],
        async_execution: Optional[bool] = False,
    ) -> Union[int, str]:
        """Prepare and execute a database query.

        Supports providing multiple substitution parameter sets, executing them
        as multiple statements sequentially.

        Supported features:
            Parameterized queries: Placeholder characters ('?') are substituted
                with values provided in `parameters`. Values are formatted to
                be properly recognized by database and to exclude SQL injection.
            Multi-statement queries: Multiple statements, provided in a single query
                and separated by semicolon, are executed separately and sequentially.
                To switch to next statement result, use `nextset` method.
            SET statements: To provide additional query execution parameters, execute
                `SET param=value` statement before it. All parameters are stored in
                cursor object until it's closed. They can also be removed with
                `flush_parameters` method call.

        Args:
            query (str): SQL query to execute.
            parameters_seq (Sequence[Sequence[ParameterType]]): A sequence of
               substitution parameter sets. Used to replace '?' placeholders inside a
               query with actual values from each set in a sequence. Resulting queries
               for each subset are executed sequentially.
            async_execution (bool): flag to determine if query should be asynchronous

        Returns:
            int|str: Query row count for synchronous execution of queries,
            query ID string for asynchronous execution.
        """
        self._do_execute(query, parameters_seq, async_execution=async_execution)
        if async_execution:
            return self.query_id
        else:
            return self.rowcount

    @check_not_closed
    def get_status(self, query_id: str) -> QueryStatus:
        """Get status of a server-side async query. Return the state of the query."""
        try:
            resp = self._api_request(
                # output_format must be empty for status to work correctly.
                # And set parameters will cause 400 errors.
                parameters={"query_id": query_id},
                path="status",
                use_set_parameters=False,
            )
            if resp.status_code == codes.BAD_REQUEST:
                raise OperationalError(
                    f"Asynchronous query {query_id} status check failed: "
                    f"{resp.status_code}."
                )
            resp_json = resp.json()
            if "status" not in resp_json:
                raise OperationalError(
                    "Invalid response to asynchronous query: missing status."
                )
        except Exception:
            self._state = CursorState.ERROR
            raise
        # Remember that query_id might be empty.
        if resp_json["status"] == "":
            return QueryStatus.NOT_READY
        return QueryStatus[resp_json["status"]]

    def close(self) -> None:
        """Terminate an ongoing query (if any) and mark connection as closed."""
        self._state = CursorState.CLOSED
        self.connection._remove_cursor(self)

    def __del__(self) -> None:
        self.close()

    # Context manager support
    @check_not_closed
    def __enter__(self) -> Cursor:
        return self

    def __exit__(
        self, exc_type: type, exc_val: Exception, exc_tb: TracebackType
    ) -> None:
        self.close()

    @check_not_closed
    def cancel(self, query_id: str) -> None:
        """Cancel a server-side async query."""
        self._api_request(
            parameters={"query_id": query_id},
            path="cancel",
            use_set_parameters=False,
        )

    # Iteration support
    @check_not_closed
    @check_query_executed
    def __iter__(self) -> Generator[List[ColType], None, None]:
        while True:
            row = self.fetchone()
            if row is None:
                return
            yield row
