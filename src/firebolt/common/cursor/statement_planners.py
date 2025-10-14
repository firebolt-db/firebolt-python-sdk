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
from firebolt.utils.exception import FireboltError, ProgrammingError

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
        return query_params


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
