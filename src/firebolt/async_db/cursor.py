from __future__ import annotations

import logging
import time
from abc import ABCMeta, abstractmethod
from functools import wraps
from types import TracebackType
from typing import (
    TYPE_CHECKING,
    Any,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)
from urllib.parse import urljoin

from httpx import URL, Headers, Response, codes

from firebolt.client.client import AsyncClient, AsyncClientV1, AsyncClientV2
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
    RESET_SESSION_HEADER,
    UPDATE_ENDPOINT_HEADER,
    UPDATE_PARAMETERS_HEADER,
    BaseCursor,
    CursorState,
    Statistics,
    _parse_update_endpoint,
    _parse_update_parameters,
    _raise_if_internal_set_parameter,
    check_not_closed,
    check_query_executed,
)
from firebolt.utils.exception import (
    EngineNotRunningError,
    FireboltDatabaseError,
    OperationalError,
    ProgrammingError,
)
from firebolt.utils.urls import DATABASES_URL, ENGINES_URL

if TYPE_CHECKING:
    from firebolt.async_db.connection import Connection

from firebolt.utils.util import _print_error_body, raise_errors_from_body

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
        if connection.init_parameters:
            self._update_set_parameters(connection.init_parameters)

    @abstractmethod
    async def _api_request(
        self,
        query: str = "",
        parameters: Optional[dict[str, Any]] = None,
        path: str = "",
        use_set_parameters: bool = True,
    ) -> Response:
        ...

    async def _raise_if_error(self, resp: Response) -> None:
        """Raise a proper error if any"""
        if resp.status_code == codes.INTERNAL_SERVER_ERROR:
            raise OperationalError(
                f"Error executing query:\n{resp.read().decode('utf-8')}"
            )
        if resp.status_code == codes.FORBIDDEN:
            if self.database and not await self.is_db_available(self.database):
                raise FireboltDatabaseError(f"Database {self.database} does not exist")
            raise ProgrammingError(resp.read().decode("utf-8"))
        if (
            resp.status_code == codes.SERVICE_UNAVAILABLE
            or resp.status_code == codes.NOT_FOUND
        ) and not await self.is_engine_running(self.engine_url):
            raise EngineNotRunningError(
                f"Firebolt engine {self.engine_url} "
                "needs to be running to run queries against it."
            )
        raise_errors_from_body(resp)
        # If no structure for error is found, log the body and raise the error
        _print_error_body(resp)
        resp.raise_for_status()

    async def _validate_set_parameter(self, parameter: SetParameter) -> None:
        """Validate parameter by executing simple query with it."""
        _raise_if_internal_set_parameter(parameter)
        resp = await self._api_request("select 1", {parameter.name: parameter.value})
        # Handle invalid set parameter
        if resp.status_code == codes.BAD_REQUEST:
            raise OperationalError(resp.text)
        await self._raise_if_error(resp)

        # set parameter passed validation
        self._set_parameters[parameter.name] = parameter.value

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

    async def _do_execute(
        self,
        raw_query: str,
        parameters: Sequence[Sequence[ParameterType]],
        skip_parsing: bool = False,
    ) -> None:
        self._reset()
        # Allow users to manually skip parsing for performance improvement.
        queries: List[Union[SetParameter, str]] = (
            [raw_query] if skip_parsing else split_format_sql(raw_query, parameters)
        )
        try:
            for query in queries:
                start_time = time.time()
                Cursor._log_query(query)

                # Define type for mypy
                row_set: Tuple[
                    int,
                    Optional[List[Column]],
                    Optional[Statistics],
                    Optional[List[List[RawColType]]],
                ] = (-1, None, None, None)
                if isinstance(query, SetParameter):
                    await self._validate_set_parameter(query)
                else:
                    resp = await self._api_request(
                        query, {"output_format": JSON_OUTPUT_FORMAT}
                    )
                    await self._raise_if_error(resp)
                    await self._parse_response_headers(resp.headers)
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
    async def execute(
        self,
        query: str,
        parameters: Optional[Sequence[ParameterType]] = None,
        skip_parsing: bool = False,
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

        Returns:
            int: Query row count.
        """
        params_list = [parameters] if parameters else []
        await self._do_execute(query, params_list, skip_parsing)
        return self.rowcount

    @check_not_closed
    async def executemany(
        self,
        query: str,
        parameters_seq: Sequence[Sequence[ParameterType]],
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

        Returns:
            int: Query row count.
        """
        await self._do_execute(query, parameters_seq)
        return self.rowcount

    @abstractmethod
    async def is_db_available(self, database: str) -> bool:
        """Verify that the database exists."""
        ...

    @abstractmethod
    async def is_engine_running(self, engine_url: str) -> bool:
        """Verify that the engine is running."""
        ...

    @wraps(BaseCursor.fetchone)
    async def fetchone(self) -> Optional[List[ColType]]:
        """Fetch the next row of a query result set."""
        return super().fetchone()

    @wraps(BaseCursor.fetchmany)
    async def fetchmany(self, size: Optional[int] = None) -> List[List[ColType]]:
        """
        Fetch the next set of rows of a query result;
        size is cursor.arraysize by default.
        """
        return super().fetchmany(size)

    @wraps(BaseCursor.fetchall)
    async def fetchall(self) -> List[List[ColType]]:
        """Fetch all remaining rows of a query result."""
        return super().fetchall()

    @wraps(BaseCursor.nextset)
    async def nextset(self) -> None:
        return super().nextset()

    # Iteration support
    @check_not_closed
    @check_query_executed
    def __aiter__(self) -> Cursor:
        return self

    # TODO: figure out how to implement __aenter__ and __await__
    @check_not_closed
    def __aenter__(self) -> Cursor:
        return self

    @check_not_closed
    def __enter__(self) -> Cursor:
        return self

    def __exit__(
        self, exc_type: type, exc_val: Exception, exc_tb: TracebackType
    ) -> None:
        self.close()

    def __await__(self) -> Iterator:
        yield None

    async def __aexit__(
        self, exc_type: type, exc_val: Exception, exc_tb: TracebackType
    ) -> None:
        self.close()

    @check_not_closed
    @check_query_executed
    async def __anext__(self) -> List[ColType]:
        row = await self.fetchone()
        if row is None:
            raise StopAsyncIteration
        return row


class CursorV2(Cursor):
    def __init__(
        self,
        *args: Any,
        client: AsyncClientV2,
        connection: Connection,
        **kwargs: Any,
    ) -> None:
        assert isinstance(client, AsyncClientV2)
        super().__init__(*args, client=client, connection=connection, **kwargs)

    async def _api_request(
        self,
        query: str = "",
        parameters: Optional[dict[str, Any]] = None,
        path: str = "",
        use_set_parameters: bool = True,
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
        parameters = parameters or {}
        if use_set_parameters:
            parameters = {**(self._set_parameters or {}), **parameters}
        if self.parameters:
            parameters = {**self.parameters, **parameters}
        return await self._client.request(
            url=urljoin(self.engine_url.rstrip("/") + "/", path or ""),
            method="POST",
            params=parameters,
            content=query,
        )

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
        super().__init__(*args, client=client, connection=connection, **kwargs)

    async def _api_request(
        self,
        query: Optional[str] = "",
        parameters: Optional[dict[str, Any]] = None,
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
        parameters = parameters or {}
        if use_set_parameters:
            parameters = {**(self._set_parameters or {}), **(parameters or {})}
        if self.parameters:
            parameters = {**self.parameters, **parameters}
        return await self._client.request(
            url=urljoin(self.engine_url.rstrip("/") + "/", path or ""),
            method="POST",
            params={
                **(parameters or dict()),
            },
            content=query,
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
