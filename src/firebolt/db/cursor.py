from __future__ import annotations

import logging
import re
import time
from abc import ABCMeta, abstractmethod
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

from httpx import URL, Response, codes

from firebolt.client import Client, ClientV1, ClientV2
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
from firebolt.db.util import ENGINE_STATUS_RUNNING
from firebolt.utils.exception import (
    AsyncExecutionUnavailableError,
    EngineNotRunningError,
    FireboltDatabaseError,
    FireboltEngineError,
    OperationalError,
    ProgrammingError,
)
from firebolt.utils.urls import DATABASES_URL, ENGINES_URL

if TYPE_CHECKING:
    from firebolt.db.connection import Connection

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
        client: Client,
        connection: Connection,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._client = client
        self.connection = connection
        if connection.database:
            self.database = connection.database

    @property
    def database(self) -> Optional[str]:
        return self.parameters.get("database")

    @database.setter
    def database(self, database: str) -> None:
        self.parameters["database"] = database

    def _raise_if_error(self, resp: Response) -> None:
        """Raise a proper error if any"""
        if resp.status_code == codes.INTERNAL_SERVER_ERROR:
            raise OperationalError(
                f"Error executing query:\n{resp.read().decode('utf-8')}"
            )
        if resp.status_code == codes.FORBIDDEN:
            if self.database and not self.is_db_available(self.database):
                raise FireboltDatabaseError(
                    f"Database {self.parameters['database']} does not exist"
                )
            raise ProgrammingError(resp.read().decode("utf-8"))
        if (
            resp.status_code == codes.SERVICE_UNAVAILABLE
            or resp.status_code == codes.NOT_FOUND
        ) and not self.is_engine_running(self.connection.engine_url):
            raise EngineNotRunningError(
                f"Firebolt engine {self.connection.engine_url} "
                "needs to be running to run queries against it."  # pragma: no mutate # noqa: E501
            )
        resp.raise_for_status()

    @abstractmethod
    def _api_request(
        self,
        query: str = "",
        parameters: Optional[dict[str, Any]] = None,
        path: str = "",
        use_set_parameters: bool = True,
    ) -> Response:
        ...

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
                    # get parameters from response
                    self._parse_response_headers(resp.headers)
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

    @abstractmethod
    def is_db_available(self, database: str) -> bool:
        """Verify that the database exists."""
        ...

    @abstractmethod
    def is_engine_running(self, engine_url: str) -> bool:
        """Verify that the engine is running."""
        ...

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

    @check_not_closed
    def __enter__(self) -> Cursor:
        return self


class CursorV2(Cursor):
    def __init__(
        self, *args: Any, client: Client, connection: Connection, **kwargs: Any
    ) -> None:
        assert isinstance(client, ClientV2)  # Type check
        super().__init__(*args, client=client, connection=connection, **kwargs)

    def _api_request(
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
        if self.connection._is_system:
            assert isinstance(self._client, ClientV2)  # Type check
            parameters["account_id"] = self._client.account_id
        return self._client.request(
            url=f"/{path}" if path else "",
            method="POST",
            params=parameters,
            content=query,
        )

    def is_db_available(self, database_name: str) -> bool:
        """
        Verify that the database exists.

        Args:
            connection (firebolt.db.connection.Connection)
            database_name (str): Name of a database
        """
        system_engine = self.connection._system_engine_connection or self.connection
        with system_engine.cursor() as cursor:
            return (
                cursor.execute(
                    """
                    SELECT 1 FROM information_schema.databases WHERE database_name=?
                    """,
                    [database_name],
                )
                > 0
            )

    def is_engine_running(self, engine_url: str) -> bool:
        """
        Verify that the engine is running.

        Args:
            connection (firebolt.db.connection.Connection): connection.
            engine_url (str): URL of the engine
        """

        if self.connection._is_system:
            # System engine is always running
            return True

        engine_name = URL(engine_url).host.split(".")[0].replace("-", "_")
        assert self.connection._system_engine_connection is not None  # Type check
        _, status, _ = self._get_engine_url_status_db(
            self.connection._system_engine_connection, engine_name
        )
        return status == ENGINE_STATUS_RUNNING

    def _get_engine_url_status_db(
        self, system_engine_connection: Connection, engine_name: str
    ) -> Tuple[str, str, str]:
        with system_engine_connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT url, attached_to, status FROM information_schema.engines
                WHERE engine_name=?
                """,
                [engine_name],
            )
            row = cursor.fetchone()
            if row is None:
                raise FireboltEngineError(
                    f"Engine with name {engine_name} doesn't exist"
                )
            engine_url, database, status = row
            return str(engine_url), str(status), str(database)  # Mypy check


class CursorV1(Cursor):
    def __init__(
        self, *args: Any, client: ClientV1, connection: Connection, **kwargs: Any
    ) -> None:
        assert isinstance(client, ClientV1)  # Type check
        super().__init__(*args, client=client, connection=connection, **kwargs)

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
        parameters = parameters or {}
        if use_set_parameters:
            parameters = {**(self._set_parameters or {}), **(parameters or {})}
        if self.parameters:
            parameters = {**self.parameters, **parameters}
        return self._client.request(
            url=f"/{path}",
            method="POST",
            params={
                **(parameters or dict()),
            },
            content=query,
        )

    def is_db_available(self, database_name: str) -> bool:
        """
        Verify that the database exists.

        Args:
            connection (firebolt.async_db.connection.Connection)
        """
        resp = self._filter_request(
            DATABASES_URL, {"filter.name_contains": database_name}
        )
        return len(resp.json()["edges"]) > 0

    def is_engine_running(self, engine_url: str) -> bool:
        """
        Verify that the engine is running.

        Args:
            connection (firebolt.async_db.connection.Connection): connection.
        """
        # Url is not guaranteed to be of this structure,
        # but for the sake of error checking this is sufficient.
        engine_name = URL(engine_url).host.split(".")[0].replace("-", "_")
        resp = self._filter_request(
            ENGINES_URL,
            {
                "filter.name_contains": engine_name,
                "filter.current_status_eq": "ENGINE_STATUS_RUNNING_REVISION_SERVING",
            },
        )
        return len(resp.json()["edges"]) > 0

    def _filter_request(self, endpoint: str, filters: dict) -> Response:
        resp = self.connection._client.request(
            # Full url overrides the client url, which contains engine as a prefix.
            url=self.connection.api_endpoint + endpoint,
            method="GET",
            params=filters,
        )
        resp.raise_for_status()
        return resp
