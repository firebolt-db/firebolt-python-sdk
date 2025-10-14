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


class BulkInsertMixin:
    """Mixin class for bulk insert functionality."""

    def create_execution_plan(
        self,
        raw_query: str,
        parameters: Sequence[Sequence[ParameterType]],
        skip_parsing: bool = False,
        async_execution: bool = False,
        streaming: bool = False,
    ) -> ExecutionPlan:
        """Create execution plan for bulk insert operations."""
        return self._create_bulk_execution_plan(
            raw_query, parameters, async_execution, streaming
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
        streaming: bool,
    ) -> ExecutionPlan:
        """
        Create bulk execution plan by delegating to
        parameter-style specific methods.
        """
        # Validate bulk_insert requirements
        self._validate_bulk_insert_query(raw_query)
        if not parameters:
            raise ProgrammingError("bulk_insert requires at least one parameter set")

        # Call the parameter-style specific bulk creation method
        return self._create_bulk_plan_impl(
            raw_query, parameters, async_execution, streaming
        )

    def _create_bulk_plan_impl(
        self,
        raw_query: str,
        parameters: Sequence[Sequence[ParameterType]],
        async_execution: bool,
        streaming: bool,
    ) -> ExecutionPlan:
        """
        Override in subclasses to provide parameter-style
        specific bulk implementation.
        """
        raise NotImplementedError("Subclass must implement _create_bulk_plan_impl")


class BaseStatementPlanner(ABC):
    """Base class for statement planning handlers."""

    def __init__(self, formatter: StatementFormatter) -> None:
        """Initialize statement planner with required dependencies."""
        self.formatter = formatter

    @abstractmethod
    def create_execution_plan(
        self,
        raw_query: str,
        parameters: Sequence[Sequence[ParameterType]],
        skip_parsing: bool = False,
        async_execution: bool = False,
        streaming: bool = False,
    ) -> ExecutionPlan:
        """Create an execution plan for the given statement and parameters."""

    @staticmethod
    def _get_output_format(streaming: bool) -> str:
        """Get output format for the query."""
        if streaming:
            return JSON_LINES_OUTPUT_FORMAT
        return JSON_OUTPUT_FORMAT


class FbNumericStatementPlanner(BaseStatementPlanner):
    """Statement planner for fb_numeric parameter style."""

    def create_execution_plan(
        self,
        raw_query: str,
        parameters: Sequence[Sequence[ParameterType]],
        skip_parsing: bool = False,
        async_execution: bool = False,
        streaming: bool = False,
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


class FbNumericBulkStatementPlanner(BulkInsertMixin, FbNumericStatementPlanner):
    """Statement planner for fb_numeric parameter style with bulk insert support."""

    def _create_bulk_plan_impl(
        self,
        raw_query: str,
        parameters: Sequence[Sequence[ParameterType]],
        async_execution: bool,
        streaming: bool,
    ) -> ExecutionPlan:
        """Create bulk insert execution plan for fb_numeric parameter style."""
        # Prepare bulk insert query and parameters for fb_numeric
        processed_query, processed_params = self._prepare_fb_numeric_bulk_insert(
            raw_query, parameters
        )

        # Build query parameters for bulk insert
        query_params = self._build_fb_numeric_query_params(
            processed_params, streaming, async_execution
        )

        return ExecutionPlan(
            queries=[processed_query],
            query_params=query_params,
            is_multi_statement=False,
            async_execution=async_execution,
            streaming=streaming,
        )

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

    def create_execution_plan(
        self,
        raw_query: str,
        parameters: Sequence[Sequence[ParameterType]],
        skip_parsing: bool = False,
        async_execution: bool = False,
        streaming: bool = False,
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


class QmarkBulkStatementPlanner(BulkInsertMixin, QmarkStatementPlanner):
    """Statement planner for qmark parameter style with bulk insert support."""

    def _create_bulk_plan_impl(
        self,
        raw_query: str,
        parameters: Sequence[Sequence[ParameterType]],
        async_execution: bool,
        streaming: bool,
    ) -> ExecutionPlan:
        """Create bulk insert execution plan for qmark parameter style."""
        # Import needed modules
        from sqlparse import parse as parse_sql  # type: ignore

        # Prepare bulk insert query for qmark style
        statements = parse_sql(raw_query)
        if not statements:
            raise ProgrammingError("Invalid SQL query for bulk insert")

        formatted_queries = []
        for param_set in parameters:
            formatted_query = self.formatter.format_statement(statements[0], param_set)
            formatted_queries.append(formatted_query)

        combined_query = "; ".join(formatted_queries)

        # Build query parameters for bulk insert
        query_params: Dict[str, Any] = {
            "output_format": self._get_output_format(streaming),
        }
        if async_execution:
            query_params["async"] = True

        return ExecutionPlan(
            queries=[combined_query],
            query_params=query_params,
            is_multi_statement=False,
            async_execution=async_execution,
            streaming=streaming,
        )


class StatementPlannerFactory:
    """Factory for creating statement planner instances based on paramstyle."""

    _PLANNER_CLASSES = {
        "fb_numeric": FbNumericStatementPlanner,
        "qmark": QmarkStatementPlanner,
    }

    _BULK_PLANNER_CLASSES = {
        "fb_numeric": FbNumericBulkStatementPlanner,
        "qmark": QmarkBulkStatementPlanner,
    }

    @classmethod
    def create_planner(
        cls, paramstyle: str, formatter: StatementFormatter, bulk_insert: bool = False
    ) -> BaseStatementPlanner:
        """Create a statement planner instance for the given paramstyle.

        Args:
            paramstyle: The parameter style ('fb_numeric' or 'qmark')
            formatter: StatementFormatter instance for statement processing
            bulk_insert: Whether to create a bulk-capable planner

        Returns:
            Appropriate statement planner instance

        Raises:
            ProgrammingError: If paramstyle is not supported
        """
        planner_classes = (
            cls._BULK_PLANNER_CLASSES if bulk_insert else cls._PLANNER_CLASSES
        )
        planner_class = planner_classes.get(paramstyle)

        if planner_class is None:
            raise ProgrammingError(f"Unsupported paramstyle: {paramstyle}")

        return planner_class(formatter)
