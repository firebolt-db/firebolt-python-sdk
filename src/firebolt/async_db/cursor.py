from __future__ import annotations

import logging
import time
import warnings
from abc import ABCMeta, abstractmethod
from types import TracebackType
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Union
from urllib.parse import urljoin

from httpx import (
    URL,
    USE_CLIENT_DEFAULT,
    Headers,
    Response,
    TimeoutException,
    codes,
)

from firebolt.client.client import AsyncClient, AsyncClientV1, AsyncClientV2
from firebolt.common._types import ColType, ParameterType, SetParameter
from firebolt.common.constants import (
    JSON_OUTPUT_FORMAT,
    RESET_SESSION_HEADER,
    UPDATE_ENDPOINT_HEADER,
    UPDATE_PARAMETERS_HEADER,
    CursorState,
)
from firebolt.common.cursor.base_cursor import (
    BaseCursor,
    _parse_update_endpoint,
    _parse_update_parameters,
    _raise_if_internal_set_parameter,
)
from firebolt.common.cursor.decorators import (
    async_not_allowed,
    check_not_closed,
    check_query_executed,
)
from firebolt.common.row_set.asynchronous.base import BaseAsyncRowSet
from firebolt.common.row_set.asynchronous.in_memory import InMemoryAsyncRowSet
from firebolt.common.row_set.asynchronous.streaming import StreamingAsyncRowSet
from firebolt.common.statement_formatter import create_statement_formatter
from firebolt.utils.exception import (
    EngineNotRunningError,
    FireboltDatabaseError,
    FireboltError,
    OperationalError,
    ProgrammingError,
    QueryTimeoutError,
    V1NotSupportedError,
)
from firebolt.utils.timeout_controller import TimeoutController
from firebolt.utils.urls import DATABASES_URL, ENGINES_URL

if TYPE_CHECKING:
    from firebolt.async_db.connection import Connection

from firebolt.utils.async_util import anext, async_islice
from firebolt.utils.util import Timer, raise_error_from_response

logger = logging.getLogger(__name__)


class Cursor(BaseCursor, metaclass=ABCMeta):
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

    def __init__(
        self,
        *args: Any,
        client: AsyncClient,
        connection: Connection,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._client = client
        self.connection = connection
        self.engine_url = connection.engine_url
        self._row_set: Optional[BaseAsyncRowSet] = None
        if connection.init_parameters:
            self._update_set_parameters(connection.init_parameters)

    async def _api_request(
        self,
        query: str = "",
        parameters: Optional[dict[str, Any]] = None,
        path: str = "",
        use_set_parameters: bool = True,
        timeout: Optional[float] = None,
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
            timeout (Optional[float]): Request execution timeout in seconds
        """
        parameters = parameters or {}
        if use_set_parameters:
            parameters = {**(self._set_parameters or {}), **parameters}
        if self.parameters:
            parameters = {**self.parameters, **parameters}
        try:
            req = self._client.build_request(
                url=urljoin(self.engine_url.rstrip("/") + "/", path or ""),
                method="POST",
                params=parameters,
                content=query,
                timeout=timeout if timeout is not None else USE_CLIENT_DEFAULT,
            )
            return await self._client.send(req, stream=True)
        except TimeoutException:
            raise QueryTimeoutError()

    async def _raise_if_error(self, resp: Response) -> None:
        """Raise a proper error if any"""
        if codes.is_error(resp.status_code):
            await resp.aread()
            if resp.status_code == codes.INTERNAL_SERVER_ERROR:
                raise OperationalError(f"Error executing query:\n{resp.text}")
            if resp.status_code == codes.FORBIDDEN:
                if self.database and not await self.is_db_available(self.database):
                    raise FireboltDatabaseError(
                        f"Database {self.database} does not exist"
                    )
                raise ProgrammingError(resp.text)
            if (
                resp.status_code == codes.SERVICE_UNAVAILABLE
                or resp.status_code == codes.NOT_FOUND
            ) and not await self.is_engine_running(self.engine_url):
                raise EngineNotRunningError(
                    f"Firebolt engine {self.engine_url} "
                    "needs to be running to run queries against it."
                )
            raise_error_from_response(resp)

    async def _validate_set_parameter(
        self, parameter: SetParameter, timeout: Optional[float]
    ) -> None:
        """Validate parameter by executing simple query with it."""
        _raise_if_internal_set_parameter(parameter)
        resp = await self._api_request(
            "select 1", {parameter.name: parameter.value}, timeout=timeout
        )
        # Handle invalid set parameter
        if resp.status_code == codes.BAD_REQUEST:
            await resp.aread()
            raise OperationalError(resp.text)
        await self._raise_if_error(resp)

        # set parameter passed validation
        self._set_parameters[parameter.name] = parameter.value

        # append empty result set
        await self._append_row_set_from_response(None)

    async def _parse_response_headers(self, headers: Headers) -> None:
        if headers.get(UPDATE_ENDPOINT_HEADER):
            endpoint, params = _parse_update_endpoint(
                headers.get(UPDATE_ENDPOINT_HEADER)
            )
            self._update_set_parameters(params)
            self.engine_url = endpoint

        if headers.get(RESET_SESSION_HEADER):
            self.flush_parameters()

        if headers.get(UPDATE_PARAMETERS_HEADER):
            param_dict = _parse_update_parameters(headers.get(UPDATE_PARAMETERS_HEADER))
            self._update_set_parameters(param_dict)

    async def _close_rowset_and_reset(self) -> None:
        """Reset cursor state."""
        if self._row_set is not None:
            await self._row_set.aclose()
        super()._reset()

    @abstractmethod
    async def execute_async(
        self,
        query: str,
        parameters: Optional[Sequence[ParameterType]] = None,
        skip_parsing: bool = False,
    ) -> int:
        """Execute a database query without maintaining a connection."""
        ...

    async def _do_execute(
        self,
        raw_query: str,
        parameters: Sequence[Sequence[ParameterType]],
        skip_parsing: bool = False,
        timeout: Optional[float] = None,
        async_execution: bool = False,
        streaming: bool = False,
    ) -> None:
        await self._close_rowset_and_reset()
        self._row_set = StreamingAsyncRowSet() if streaming else InMemoryAsyncRowSet()
        queries: List[Union[SetParameter, str]] = (
            [raw_query]
            if skip_parsing
            else self._formatter.split_format_sql(raw_query, parameters)
        )
        timeout_controller = TimeoutController(timeout)

        if len(queries) > 1 and async_execution:
            raise FireboltError(
                "Server side async does not support multi-statement queries"
            )
        try:
            for query in queries:
                await self._execute_single_query(
                    query, timeout_controller, async_execution, streaming
                )
            self._state = CursorState.DONE
        except Exception:
            self._state = CursorState.ERROR
            raise

    async def _execute_single_query(
        self,
        query: Union[SetParameter, str],
        timeout_controller: TimeoutController,
        async_execution: bool,
        streaming: bool,
    ) -> None:
        start_time = time.time()
        Cursor._log_query(query)
        timeout_controller.raise_if_timeout()

        if isinstance(query, SetParameter):
            if async_execution:
                raise FireboltError(
                    "Server side async does not support set statements, "
                    "please use execute to set this parameter"
                )
            await self._validate_set_parameter(query, timeout_controller.remaining())
        else:
            await self._handle_query_execution(
                query, timeout_controller, async_execution, streaming
            )

        if not async_execution:
            logger.info(
                f"Query fetched {self.rowcount} rows in"
                f" {time.time() - start_time} seconds."
            )
        else:
            logger.info("Query submitted for async execution.")

    async def _handle_query_execution(
        self,
        query: str,
        timeout_controller: TimeoutController,
        async_execution: bool,
        streaming: bool,
    ) -> None:
        query_params: Dict[str, Any] = {
            "output_format": self._get_output_format(streaming)
        }
        if async_execution:
            query_params["async"] = True
        resp = await self._api_request(
            query,
            query_params,
            timeout=timeout_controller.remaining(),
        )
        await self._raise_if_error(resp)
        if async_execution:
            await resp.aread()
            self._parse_async_response(resp)
        else:
            await self._parse_response_headers(resp.headers)
            await self._append_row_set_from_response(resp)

    @check_not_closed
    async def execute(
        self,
        query: str,
        parameters: Optional[Sequence[ParameterType]] = None,
        skip_parsing: bool = False,
        timeout_seconds: Optional[float] = None,
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
            timeout_seconds (Optional[float]): Query execution timeout in seconds

        Returns:
            int: Query row count.
        """
        params_list = [parameters] if parameters else []
        await self._do_execute(
            query, params_list, skip_parsing, timeout=timeout_seconds
        )
        return self.rowcount

    @check_not_closed
    async def executemany(
        self,
        query: str,
        parameters_seq: Sequence[Sequence[ParameterType]],
        timeout_seconds: Optional[float] = None,
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
            timeout_seconds (Optional[float]): Query execution timeout in seconds.

        Returns:
            int: Query row count.
        """
        await self._do_execute(query, parameters_seq, timeout=timeout_seconds)
        return self.rowcount

    @check_not_closed
    async def execute_stream(
        self,
        query: str,
        parameters: Optional[Sequence[ParameterType]] = None,
        skip_parsing: bool = False,
    ) -> None:
        """Prepare and execute a database query, with streaming results.

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
            parameters (Optional[Sequence[ParameterType]]): Substitution parameters.
                Used to replace '?' placeholders inside a query with actual values.
            skip_parsing (bool): Flag to disable query parsing. This will
                disable parameterized, multi-statement and SET queries,
                while improving performance
        """
        params_list = [parameters] if parameters else []
        await self._do_execute(query, params_list, skip_parsing, streaming=True)

    async def _append_row_set_from_response(
        self,
        response: Optional[Response],
    ) -> None:
        """Store information about executed query."""
        if self._row_set is None:
            raise OperationalError("Row set is not initialized.")
        if response is None:
            self._row_set.append_empty_response()
        else:
            await self._row_set.append_response(response)

    _performance_log_message = (
        "[PERFORMANCE] Parsing query output into native Python types"
    )

    @check_not_closed
    @async_not_allowed
    @check_query_executed
    async def fetchone(self) -> Optional[List[ColType]]:
        """Fetch the next row of a query result set."""
        assert self._row_set is not None
        with Timer(self._performance_log_message):
            return await anext(self._row_set, None)

    @check_not_closed
    @async_not_allowed
    @check_query_executed
    async def fetchmany(self, size: Optional[int] = None) -> List[List[ColType]]:
        """
        Fetch the next set of rows of a query result;
        cursor.arraysize is default size.
        """
        assert self._row_set is not None
        size = size if size is not None else self.arraysize
        with Timer(self._performance_log_message):
            return await async_islice(self._row_set, size)

    @check_not_closed
    @async_not_allowed
    @check_query_executed
    async def fetchall(self) -> List[List[ColType]]:
        """Fetch all remaining rows of a query result."""
        assert self._row_set is not None
        with Timer(self._performance_log_message):
            return [it async for it in self._row_set]

    @check_not_closed
    @async_not_allowed
    @check_query_executed
    async def nextset(self) -> bool:
        """
        Skip to the next available set, discarding any remaining rows
        from the current set.

        Returns:
            bool: True if there is a next result set, False otherwise
        """
        assert self._row_set is not None
        return await self._row_set.nextset()

    async def aclose(self) -> None:
        super().close()
        if self._row_set is not None:
            await self._row_set.aclose()

    @abstractmethod
    async def is_db_available(self, database: str) -> bool:
        """Verify that the database exists."""
        ...

    @abstractmethod
    async def is_engine_running(self, engine_url: str) -> bool:
        """Verify that the engine is running."""
        ...

    # Iteration support
    @check_not_closed
    @async_not_allowed
    @check_query_executed
    def __aiter__(self) -> Cursor:
        return self

    @check_not_closed
    async def __aenter__(self) -> Cursor:
        return self

    async def __aexit__(
        self, exc_type: type, exc_val: Exception, exc_tb: TracebackType
    ) -> None:
        await self.aclose()

    @check_not_closed
    @async_not_allowed
    @check_query_executed
    async def __anext__(self) -> List[ColType]:
        assert self._row_set is not None
        return await self._row_set.__anext__()

    @check_not_closed
    def __enter__(self) -> Cursor:
        warnings.warn(
            "Using __enter__ is deprecated, use 'async with' instead",
            DeprecationWarning,
        )
        return self

    def __exit__(
        self, exc_type: type, exc_val: Exception, exc_tb: TracebackType
    ) -> None:
        return None


class CursorV2(Cursor):
    def __init__(
        self,
        *args: Any,
        client: AsyncClientV2,
        connection: Connection,
        **kwargs: Any,
    ) -> None:
        assert isinstance(client, AsyncClientV2)
        super().__init__(
            *args,
            client=client,
            connection=connection,
            formatter=create_statement_formatter(version=2),
            **kwargs,
        )

    @check_not_closed
    async def execute_async(
        self,
        query: str,
        parameters: Optional[Sequence[ParameterType]] = None,
        skip_parsing: bool = False,
    ) -> int:
        """
        Execute a database query without maintating a connection.

        Supported features:
            Parameterized queries: placeholder characters ('?') are substituted
                with values provided in `parameters`. Values are formatted to
                be properly recognized by database and to exclude SQL injection.

        Not supported:
            Multi-statement queries: multiple statements, provided in a single query
                and separated by semicolon.
            SET statements: to provide additional query execution parameters, execute
                `SET param=value` statement before it. Use `execute` method to set
                parameters.

        Args:
            query (str): SQL query to execute
            parameters (Optional[Sequence[ParameterType]]): A sequence of substitution
                parameters. Used to replace '?' placeholders inside a query with
                actual values
            skip_parsing (bool): Flag to disable query parsing. This will
                disable parameterized queries while potentially improving performance

        Returns:
            int: Always returns -1, as async execution does not return row count.
        """
        await self._do_execute(
            query,
            [parameters] if parameters else [],
            skip_parsing,
            async_execution=True,
        )
        return -1

    async def is_db_available(self, database_name: str) -> bool:
        """
        Verify that the database exists.

        Args:
            connection (firebolt.async_db.connection.Connection)
            database_name (str): Name of a database
        """
        # For v2 accounts if we're connected it automatically
        # means the database is available
        return True

    async def is_engine_running(self, engine_url: str) -> bool:
        """
        Verify that the engine is running.

        Args:
            connection (firebolt.async_db.connection.Connection): connection.
            engine_url (str): URL of the engine
        """
        # For v2 accounts we don't have the engine context,
        # so we can't check if it's running
        return True


class CursorV1(Cursor):
    def __init__(
        self,
        *args: Any,
        client: AsyncClientV1,
        connection: Connection,
        **kwargs: Any,
    ) -> None:
        assert isinstance(client, AsyncClientV1)
        super().__init__(
            *args,
            client=client,
            connection=connection,
            formatter=create_statement_formatter(version=1),
            **kwargs,
        )

    async def is_db_available(self, database_name: str) -> bool:
        """
        Verify that the database exists.

        Args:
            connection (firebolt.async_db.connection.Connection)
        """
        resp = await self._filter_request(
            DATABASES_URL, {"filter.name_contains": database_name}
        )
        return len(resp.json()["edges"]) > 0

    async def is_engine_running(self, engine_url: str) -> bool:
        """
        Verify that the engine is running.

        Args:
            connection (firebolt.async_db.connection.Connection): connection.
        """
        # Url is not guaranteed to be of this structure,
        # but for the sake of error checking this is sufficient.
        engine_name = URL(engine_url).host.split(".")[0].replace("-", "_")
        resp = await self._filter_request(
            ENGINES_URL,
            {
                "filter.name_contains": engine_name,
                "filter.current_status_eq": "ENGINE_STATUS_RUNNING_REVISION_SERVING",
            },
        )
        return len(resp.json()["edges"]) > 0

    async def _filter_request(self, endpoint: str, filters: dict) -> Response:
        resp = await self.connection._client.request(
            # Full url overrides the client url, which contains engine as a prefix.
            url=self.connection.api_endpoint + endpoint,
            method="GET",
            params=filters,
        )
        resp.raise_for_status()
        return resp

    async def execute_async(
        self,
        query: str,
        parameters: Optional[Sequence[ParameterType]] = None,
        skip_parsing: bool = False,
    ) -> int:
        raise V1NotSupportedError("Async execution")

    async def execute_stream(
        self,
        query: str,
        parameters: Optional[Sequence[ParameterType]] = None,
        skip_parsing: bool = False,
    ) -> None:
        raise V1NotSupportedError("Query result streaming")

    @staticmethod
    def _get_output_format(is_streaming: bool) -> str:
        """Get output format."""
        # Streaming is not supported in v1
        return JSON_OUTPUT_FORMAT
