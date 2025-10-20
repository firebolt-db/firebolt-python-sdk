from __future__ import annotations

"""Statement planning handlers for different parameter styles."""

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Union

from firebolt.common._types import ParameterType, SetParameter
from firebolt.common.constants import (
    JSON_LINES_OUTPUT_FORMAT,
    JSON_OUTPUT_FORMAT,
)
from firebolt.utils.exception import (
    ConfigurationError,
    FireboltError,
    ProgrammingError,
)

if TYPE_CHECKING:
    from firebolt.common.statement_formatter import StatementFormatter


@dataclass
class ExecutionPlan:
    """Represents a plan for executing queries."""

    queries: List[Union[SetParameter, str]]
    query_params: Optional[Dict[str, Any]] = None
    is_multi_statement: bool = False
    async_execution: bool = False
    streaming: bool = False


class BaseStatementPlanner(ABC):
    """Base class for statement planning handlers."""

    def __init__(self, formatter: StatementFormatter) -> None:
        """Initialize statement planner with required dependencies."""
        self.formatter = formatter

    def create_execution_plan(
        self,
        raw_query: str,
        parameters: Sequence[Sequence[ParameterType]],
        skip_parsing: bool = False,
        async_execution: bool = False,
        streaming: bool = False,
        bulk_insert: bool = False,
    ) -> ExecutionPlan:
        """Create an execution plan for a given statement and parameters.

        This method serves as a factory for creating an execution plan, which
        encapsulates the queries to be executed and the parameters for execution.
        It supports standard execution, as well as bulk insert, which is handled
        by a separate method.

        Args:
            raw_query (str): The raw SQL query to be executed.
            parameters (Sequence[Sequence[ParameterType]]): A sequence of parameter
                sequences for the query.
            skip_parsing (bool): If True, the query will not be parsed, and all
                special features (e.g., multi-statement, parameterized queries) will
                be disabled. Defaults to False.
            async_execution (bool): If True, the query will be executed
                asynchronously. Defaults to False.
            streaming (bool): If True, the query results will be streamed.
                Defaults to False.
            bulk_insert (bool): If True, the query will be treated as a bulk insert
                operation. Defaults to False.

        Returns:
            ExecutionPlan: An object representing the execution plan.
        """
        if bulk_insert:
            return self._create_bulk_execution_plan(
                raw_query, parameters, async_execution
            )
        else:
            return self._create_standard_execution_plan(
                raw_query, parameters, skip_parsing, async_execution, streaming
            )

    def _validate_bulk_insert_query(self, query: str) -> None:
        """Validate that query is an INSERT statement for bulk_insert."""
        query_normalized = query.lstrip().lower()
        if not query_normalized.startswith("insert"):
            raise ConfigurationError(
                "bulk_insert is only supported for INSERT statements"
            )
        if ";" in query.strip().rstrip(";"):
            raise ProgrammingError(
                "bulk_insert does not support multi-statement queries"
            )

    def _create_bulk_execution_plan(
        self,
        raw_query: str,
        parameters: Sequence[Sequence[ParameterType]],
        async_execution: bool,
    ) -> ExecutionPlan:
        """Create bulk execution plan using formatter logic."""
        # Validate bulk_insert requirements
        self._validate_bulk_insert_query(raw_query)
        if not parameters:
            raise ProgrammingError("bulk_insert requires at least one parameter set")

        return self._create_bulk_plan_impl(raw_query, parameters, async_execution)

    @abstractmethod
    def _create_standard_execution_plan(
        self,
        raw_query: str,
        parameters: Sequence[Sequence[ParameterType]],
        skip_parsing: bool,
        async_execution: bool,
        streaming: bool,
    ) -> ExecutionPlan:
        """Create standard (non-bulk) execution plan."""

    @abstractmethod
    def _create_bulk_plan_impl(
        self,
        raw_query: str,
        parameters: Sequence[Sequence[ParameterType]],
        async_execution: bool,
    ) -> ExecutionPlan:
        """Create parameter-style specific bulk execution plan."""

    @staticmethod
    def _get_output_format(streaming: bool) -> str:
        """Get output format for the query."""
        if streaming:
            return JSON_LINES_OUTPUT_FORMAT
        return JSON_OUTPUT_FORMAT


class FbNumericStatementPlanner(BaseStatementPlanner):
    """Statement planner for fb_numeric parameter style."""

    def _create_standard_execution_plan(
        self,
        raw_query: str,
        parameters: Sequence[Sequence[ParameterType]],
        skip_parsing: bool,
        async_execution: bool,
        streaming: bool,
    ) -> ExecutionPlan:
        """Create execution plan for fb_numeric parameter style."""
        query_params = self._build_fb_numeric_query_params(
            parameters, streaming, async_execution
        )

        return ExecutionPlan(
            queries=[raw_query],
            query_params=query_params,
            is_multi_statement=False,
            async_execution=async_execution,
            streaming=streaming,
        )

    def _create_bulk_plan_impl(
        self,
        raw_query: str,
        parameters: Sequence[Sequence[ParameterType]],
        async_execution: bool,
    ) -> ExecutionPlan:
        """Create bulk insert execution plan for fb_numeric parameter style."""
        # Prepare bulk insert query and parameters for fb_numeric
        processed_query, processed_params = self._prepare_fb_numeric_bulk_insert(
            raw_query, parameters
        )

        # Build query parameters for bulk insert
        query_params = self._build_fb_numeric_query_params(
            processed_params, streaming=False, async_execution=async_execution
        )

        return ExecutionPlan(
            queries=[processed_query],
            query_params=query_params,
            is_multi_statement=False,
            async_execution=async_execution,
            streaming=False,
        )

    def _build_fb_numeric_query_params(
        self,
        parameters: Sequence[Sequence[ParameterType]],
        streaming: bool,
        async_execution: bool,
        extra_params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Build query parameters for fb_numeric style."""
        param_list = parameters[0] if parameters else []
        query_parameters = [
            {
                "name": f"${i+1}",
                "value": self.formatter.convert_parameter_for_serialization(value),
            }
            for i, value in enumerate(param_list)
        ]

        query_params: Dict[str, Any] = {
            "output_format": self._get_output_format(streaming),
        }
        if query_parameters:
            query_params["query_parameters"] = json.dumps(query_parameters)
        if async_execution:
            query_params["async"] = True
        if extra_params:
            query_params.update(extra_params)
        return query_params

    def _prepare_fb_numeric_bulk_insert(
        self, query: str, parameters_seq: Sequence[Sequence[ParameterType]]
    ) -> tuple[str, Sequence[Sequence[ParameterType]]]:
        """Prepare multiple INSERT queries as a single batch for fb_numeric style."""
        # For bulk insert, we need to create unique parameter names for each INSERT
        # Example: ($1, $2); ($3, $4); ($5, $6) instead of ($1, $2); ($1, $2); ($1, $2)
        queries = []
        param_offset = 0
        for param_set in parameters_seq:
            # Replace parameter placeholders with unique numbers
            modified_query = query
            for i in range(len(param_set)):
                old_param = f"${i + 1}"
                new_param = f"${param_offset + i + 1}"
                modified_query = modified_query.replace(old_param, new_param)
            queries.append(modified_query)
            param_offset += len(param_set)

        combined_query = "; ".join(queries)
        flattened_parameters = [
            param for param_set in parameters_seq for param in param_set
        ]
        return combined_query, [flattened_parameters]


class QmarkStatementPlanner(BaseStatementPlanner):
    """Statement planner for qmark parameter style."""

    def _create_standard_execution_plan(
        self,
        raw_query: str,
        parameters: Sequence[Sequence[ParameterType]],
        skip_parsing: bool,
        async_execution: bool,
        streaming: bool,
    ) -> ExecutionPlan:
        """Create execution plan for qmark parameter style."""
        queries: List[Union[SetParameter, str]] = (
            [raw_query]
            if skip_parsing
            else self.formatter.split_format_sql(raw_query, parameters)
        )

        if len(queries) > 1 and async_execution:
            raise FireboltError(
                "Server side async does not support multi-statement queries"
            )

        # Build basic query parameters for qmark style
        query_params: Dict[str, Any] = {
            "output_format": self._get_output_format(streaming),
        }
        if async_execution:
            query_params["async"] = True

        return ExecutionPlan(
            queries=queries,
            query_params=query_params,
            is_multi_statement=len(queries) > 1,
            async_execution=async_execution,
            streaming=streaming,
        )

    def _create_bulk_plan_impl(
        self,
        raw_query: str,
        parameters: Sequence[Sequence[ParameterType]],
        async_execution: bool,
    ) -> ExecutionPlan:
        """Create bulk insert execution plan for qmark parameter style."""
        # Use formatter's bulk insert method to create combined query
        combined_query = self.formatter.format_bulk_insert(raw_query, parameters)

        # Build query parameters for bulk insert
        query_params: Dict[str, Any] = {
            "output_format": self._get_output_format(False),
        }
        if async_execution:
            query_params["async"] = True

        return ExecutionPlan(
            queries=[combined_query],
            query_params=query_params,
            is_multi_statement=False,
            async_execution=async_execution,
            streaming=False,
        )


class StatementPlannerFactory:
    """Factory for creating statement planner instances based on paramstyle."""

    _PLANNER_CLASSES = {
        "fb_numeric": FbNumericStatementPlanner,
        "qmark": QmarkStatementPlanner,
    }

    @classmethod
    def create_planner(
        cls, paramstyle: str, formatter: StatementFormatter
    ) -> BaseStatementPlanner:
        """Create a statement planner instance for the given paramstyle.

        Args:
            paramstyle: The parameter style ('fb_numeric' or 'qmark')
            formatter: StatementFormatter instance for statement processing

        Returns:
            Appropriate statement planner instance

        Raises:
            ProgrammingError: If paramstyle is not supported
        """
        planner_class = cls._PLANNER_CLASSES.get(paramstyle)

        if planner_class is None:
            raise ProgrammingError(f"Unsupported paramstyle: {paramstyle}")

        return planner_class(formatter)
