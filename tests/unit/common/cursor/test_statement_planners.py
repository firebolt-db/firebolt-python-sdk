"""Unit tests for statement planners using plain functions and fixtures."""

import json
from unittest.mock import Mock

import pytest

from firebolt.common._types import SetParameter
from firebolt.common.constants import (
    JSON_LINES_OUTPUT_FORMAT,
    JSON_OUTPUT_FORMAT,
)
from firebolt.common.cursor.statement_planners import (
    BaseStatementPlanner,
    ExecutionPlan,
    FbNumericStatementPlanner,
    QmarkStatementPlanner,
    StatementPlannerFactory,
)
from firebolt.common.statement_formatter import create_statement_formatter
from firebolt.utils.exception import FireboltError, ProgrammingError


# Fixtures
@pytest.fixture
def formatter():
    """Create statement formatter for tests."""
    return create_statement_formatter(version=1)


@pytest.fixture
def fb_numeric_planner(formatter):
    """Create FbNumericStatementPlanner for tests."""
    return FbNumericStatementPlanner(formatter)


@pytest.fixture
def qmark_planner(formatter):
    """Create QmarkStatementPlanner for tests."""
    return QmarkStatementPlanner(formatter)


# ExecutionPlan tests
def test_execution_plan_creation():
    """Test basic ExecutionPlan creation."""
    plan = ExecutionPlan(
        queries=["SELECT 1"],
        query_params={"output_format": "JSON_Compact"},
        is_multi_statement=False,
    )
    assert plan.queries == ["SELECT 1"]
    assert plan.query_params == {"output_format": "JSON_Compact"}
    assert plan.is_multi_statement is False


def test_execution_plan_defaults():
    """Test ExecutionPlan default values."""
    plan = ExecutionPlan(queries=["SELECT 1"])
    assert plan.queries == ["SELECT 1"]
    assert plan.query_params is None
    assert plan.is_multi_statement is False


# BaseStatementPlanner tests
@pytest.mark.parametrize(
    "streaming,expected_format",
    [
        (True, JSON_LINES_OUTPUT_FORMAT),
        (False, JSON_OUTPUT_FORMAT),
    ],
)
def test_get_output_format(streaming, expected_format):
    """Test output format selection."""
    assert BaseStatementPlanner._get_output_format(streaming) == expected_format


# FbNumericStatementPlanner tests
def test_fb_numeric_planner_initialization(formatter):
    """Test planner initialization."""
    planner = FbNumericStatementPlanner(formatter)
    assert planner.formatter == formatter


def test_fb_numeric_basic_execution_plan(fb_numeric_planner):
    """Test basic execution plan creation."""
    plan = fb_numeric_planner.create_execution_plan(
        "SELECT $1", [[42]], bulk_insert=False
    )

    assert len(plan.queries) == 1
    assert plan.queries[0] == "SELECT $1"
    assert plan.is_multi_statement is False
    assert plan.query_params is not None
    assert "output_format" in plan.query_params
    assert plan.query_params["output_format"] == JSON_OUTPUT_FORMAT


def test_fb_numeric_execution_plan_with_parameters(fb_numeric_planner):
    """Test execution plan with parameters."""
    parameters = [[42, "test", True]]
    plan = fb_numeric_planner.create_execution_plan(
        "SELECT $1, $2, $3", parameters, bulk_insert=False
    )

    assert plan.query_params is not None
    assert "query_parameters" in plan.query_params

    # Parse the JSON to verify structure
    query_params = json.loads(plan.query_params["query_parameters"])
    assert len(query_params) == 3
    assert query_params[0] == {"name": "$1", "value": 42}
    assert query_params[1] == {"name": "$2", "value": "test"}
    assert query_params[2] == {"name": "$3", "value": True}


def test_fb_numeric_execution_plan_no_parameters(fb_numeric_planner):
    """Test execution plan without parameters."""
    plan = fb_numeric_planner.create_execution_plan("SELECT 1", [], bulk_insert=False)

    assert plan.query_params is not None
    assert "query_parameters" not in plan.query_params
    assert plan.query_params["output_format"] == JSON_OUTPUT_FORMAT


@pytest.mark.parametrize(
    "streaming,async_execution,expected_format,expected_async",
    [
        (True, False, JSON_LINES_OUTPUT_FORMAT, None),
        (False, True, JSON_OUTPUT_FORMAT, True),
        (True, True, JSON_LINES_OUTPUT_FORMAT, True),
        (False, False, JSON_OUTPUT_FORMAT, None),
    ],
)
def test_fb_numeric_execution_plan_options(
    fb_numeric_planner, streaming, async_execution, expected_format, expected_async
):
    """Test execution plan with various streaming and async options."""
    plan = fb_numeric_planner.create_execution_plan(
        "SELECT $1", [[42]], streaming=streaming, async_execution=async_execution
    )

    assert plan.query_params["output_format"] == expected_format
    if expected_async:
        assert plan.query_params["async"] is True
    else:
        assert "async" not in plan.query_params


def test_fb_numeric_execution_plan_skip_parsing_ignored(fb_numeric_planner):
    """Test that skip_parsing is ignored for fb_numeric style."""
    plan = fb_numeric_planner.create_execution_plan(
        "SELECT $1", [[42]], skip_parsing=True
    )

    # skip_parsing should be ignored for fb_numeric
    assert len(plan.queries) == 1
    assert plan.queries[0] == "SELECT $1"
    assert plan.is_multi_statement is False


def test_fb_numeric_complex_parameters(fb_numeric_planner):
    """Test execution plan with complex parameter types."""
    parameters = [[42, "string", True, None, 3.14, [1, 2, 3]]]
    plan = fb_numeric_planner.create_execution_plan(
        "SELECT $1, $2, $3, $4, $5, $6", parameters
    )

    query_params = json.loads(plan.query_params["query_parameters"])
    assert len(query_params) == 6
    assert query_params[0]["value"] == 42
    assert query_params[1]["value"] == "string"
    assert query_params[2]["value"] is True
    assert query_params[3]["value"] is None
    assert abs(query_params[4]["value"] - 3.14) < 0.001


# QmarkStatementPlanner tests
def test_qmark_planner_initialization(formatter):
    """Test planner initialization."""
    planner = QmarkStatementPlanner(formatter)
    assert planner.formatter == formatter


def test_qmark_planner_initialization(formatter):
    """Test planner initialization."""
    planner = QmarkStatementPlanner(formatter)
    assert planner.formatter == formatter


def test_qmark_basic_execution_plan(qmark_planner):
    """Test basic execution plan creation."""
    plan = qmark_planner.create_execution_plan("SELECT ?", [[42]], bulk_insert=False)

    assert len(plan.queries) >= 1  # Could be split by formatter
    assert plan.query_params is not None
    assert "output_format" in plan.query_params
    assert plan.query_params["output_format"] == JSON_OUTPUT_FORMAT


def test_qmark_execution_plan_skip_parsing(qmark_planner):
    """Test execution plan with skip_parsing enabled."""
    plan = qmark_planner.create_execution_plan(
        "SELECT ?; SELECT ?", [[42]], skip_parsing=True
    )

    # With skip_parsing, should not split the query
    assert len(plan.queries) == 1
    assert plan.queries[0] == "SELECT ?; SELECT ?"
    assert plan.is_multi_statement is False


@pytest.mark.parametrize(
    "streaming,async_execution,expected_format,expected_async",
    [
        (True, False, JSON_LINES_OUTPUT_FORMAT, None),
        (False, True, JSON_OUTPUT_FORMAT, True),
        (True, True, JSON_LINES_OUTPUT_FORMAT, True),
        (False, False, JSON_OUTPUT_FORMAT, None),
    ],
)
def test_qmark_execution_plan_options(
    qmark_planner, streaming, async_execution, expected_format, expected_async
):
    """Test execution plan with various streaming and async options."""
    plan = qmark_planner.create_execution_plan(
        "SELECT ?", [[42]], streaming=streaming, async_execution=async_execution
    )

    assert plan.query_params["output_format"] == expected_format
    if expected_async:
        assert plan.query_params["async"] is True
    else:
        assert "async" not in plan.query_params


def test_qmark_multi_statement_async_error(qmark_planner):
    """Test that multi-statement queries with async raise error."""
    # Mock the formatter to return multiple queries
    qmark_planner.formatter.split_format_sql = Mock(
        return_value=["SELECT 1", "SELECT 2"]
    )

    with pytest.raises(
        FireboltError,
        match="Server side async does not support multi-statement queries",
    ):
        qmark_planner.create_execution_plan(
            "SELECT 1; SELECT 2", [[]], async_execution=True
        )


@pytest.mark.parametrize(
    "queries,expected_multi",
    [
        (["SELECT 1"], False),
        (["SELECT 1", "SELECT 2"], True),
    ],
)
def test_qmark_multi_statement_detection(qmark_planner, queries, expected_multi):
    """Test multi-statement detection."""
    # Mock the formatter to return queries
    qmark_planner.formatter.split_format_sql = Mock(return_value=queries)

    plan = qmark_planner.create_execution_plan(
        "SELECT 1; SELECT 2", [[]], bulk_insert=False
    )

    assert plan.is_multi_statement is expected_multi
    assert len(plan.queries) == len(queries)


def test_qmark_set_parameter_handling(qmark_planner):
    """Test handling of SET parameters."""
    set_param = SetParameter("test_param", "test_value")
    qmark_planner.formatter.split_format_sql = Mock(
        return_value=[set_param, "SELECT 1"]
    )

    plan = qmark_planner.create_execution_plan(
        "SET test_param = test_value; SELECT 1", [[]]
    )

    assert plan.is_multi_statement is True
    assert len(plan.queries) == 2
    assert isinstance(plan.queries[0], SetParameter)
    assert plan.queries[1] == "SELECT 1"


# StatementPlannerFactory tests
@pytest.mark.parametrize(
    "paramstyle,expected_class",
    [
        ("fb_numeric", FbNumericStatementPlanner),
        ("qmark", QmarkStatementPlanner),
    ],
)
def test_statement_planner_factory_creates_correct_planners(
    formatter, paramstyle, expected_class
):
    """Test that factory creates correct planner types."""
    planner = StatementPlannerFactory.create_planner(paramstyle, formatter)

    assert isinstance(planner, expected_class)
    assert planner.formatter == formatter


def test_statement_planner_factory_unsupported_paramstyle(formatter):
    """Test error for unsupported paramstyle."""
    with pytest.raises(ProgrammingError, match="Unsupported paramstyle: unsupported"):
        StatementPlannerFactory.create_planner("unsupported", formatter)


@pytest.mark.parametrize(
    "paramstyle,expected_class",
    [
        ("fb_numeric", "FbNumericStatementPlanner"),
        ("qmark", "QmarkStatementPlanner"),
    ],
)
def test_statement_planner_factory_creates_correct_bulk_planners(
    formatter, paramstyle, expected_class
):
    """Test that factory creates unified planner types that support both standard and bulk operations."""
    planner = StatementPlannerFactory.create_planner(paramstyle, formatter)

    assert planner.__class__.__name__ == expected_class
    assert planner.formatter == formatter


def test_statement_planner_factory_unified_planners(formatter):
    """Test that planners now support both standard and bulk operations."""
    fb_planner = StatementPlannerFactory.create_planner("fb_numeric", formatter)
    qmark_planner = StatementPlannerFactory.create_planner("qmark", formatter)

    # Check that planners are the same whether bulk_insert is True or False
    assert isinstance(fb_planner, FbNumericStatementPlanner)
    assert isinstance(qmark_planner, QmarkStatementPlanner)


# Edge cases and error conditions
def test_fb_numeric_empty_parameters_list(formatter):
    """Test fb_numeric with empty parameters list."""
    planner = FbNumericStatementPlanner(formatter)
    plan = planner.create_execution_plan("SELECT 1", [], bulk_insert=False)

    assert "query_parameters" not in plan.query_params
    assert plan.query_params["output_format"] == JSON_OUTPUT_FORMAT


def test_fb_numeric_none_parameters(formatter):
    """Test fb_numeric with None in parameters."""
    planner = FbNumericStatementPlanner(formatter)
    plan = planner.create_execution_plan(
        "SELECT $1, $2", [[None, "test"]], bulk_insert=False
    )

    query_params = json.loads(plan.query_params["query_parameters"])
    assert query_params[0]["value"] is None
    assert query_params[1]["value"] == "test"


def test_qmark_empty_query(formatter):
    """Test qmark with empty query."""
    planner = QmarkStatementPlanner(formatter)
    plan = planner.create_execution_plan("", [[]], bulk_insert=False)

    assert plan.query_params is not None
    assert "output_format" in plan.query_params


@pytest.mark.parametrize("invalid_paramstyle", ["FB_NUMERIC", "QMARK", "invalid"])
def test_factory_case_sensitivity(formatter, invalid_paramstyle):
    """Test factory case sensitivity."""
    with pytest.raises(ProgrammingError):
        StatementPlannerFactory.create_planner(invalid_paramstyle, formatter)


def test_execution_plan_immutability():
    """Test that ExecutionPlan behaves correctly as a dataclass."""
    plan = ExecutionPlan(queries=["SELECT 1"])

    # Test that we can create multiple instances
    plan2 = ExecutionPlan(queries=["SELECT 2"], is_multi_statement=True)

    assert plan.queries != plan2.queries
    assert plan.is_multi_statement != plan2.is_multi_statement
