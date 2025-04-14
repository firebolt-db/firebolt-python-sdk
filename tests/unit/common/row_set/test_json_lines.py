from copy import deepcopy
from typing import Any, Dict, Type

from pytest import mark, raises

from firebolt.common.row_set.json_lines import (
    Column,
    DataRecord,
    ErrorRecord,
    JSONLinesRecord,
    StartRecord,
    SuccessRecord,
    parse_json_lines_record,
)
from firebolt.common.row_set.types import Statistics
from firebolt.utils.exception import OperationalError


@mark.parametrize(
    "record_data,expected_type,message_type_value",
    [
        (
            {
                "message_type": "START",
                "result_columns": [{"name": "col1", "type": "int"}],
                "query_id": "query_id",
                "query_label": "query_label",
                "request_id": "request_id",
            },
            StartRecord,
            "START",
        ),
        (
            {
                "message_type": "DATA",
                "data": [[1, 2, 3]],
            },
            DataRecord,
            "DATA",
        ),
        (
            {
                "message_type": "FINISH_SUCCESSFULLY",
                "statistics": {
                    "elapsed": 0.1,
                    "rows_read": 10,
                    "bytes_read": 100,
                    "time_before_execution": 0.01,
                    "time_to_execute": 0.09,
                },
            },
            SuccessRecord,
            "FINISH_SUCCESSFULLY",
        ),
        (
            {
                "message_type": "FINISH_WITH_ERROR",
                "errors": [{"message": "error message", "code": 123}],
                "query_id": "query_id",
                "query_label": "query_label",
                "request_id": "request_id",
                "statistics": {
                    "elapsed": 0.1,
                    "rows_read": 10,
                    "bytes_read": 100,
                    "time_before_execution": 0.01,
                    "time_to_execute": 0.09,
                },
            },
            ErrorRecord,
            "FINISH_WITH_ERROR",
        ),
    ],
)
def test_parse_json_lines_record(
    record_data: Dict[str, Any],
    expected_type: Type[JSONLinesRecord],
    message_type_value: str,
):
    """Test that parse_json_lines_record correctly parses various record types."""
    # Copy the record to avoid modifying the original during parsing
    record_data_copy = deepcopy(record_data)
    record = parse_json_lines_record(record_data_copy)

    # Verify common properties
    assert isinstance(record, expected_type)
    assert record.message_type == message_type_value

    # Verify type-specific properties
    if expected_type == StartRecord:
        result_columns = record_data["result_columns"]
        assert record.query_id == record_data["query_id"]
        assert record.query_label == record_data["query_label"]
        assert record.request_id == record_data["request_id"]
        assert len(record.result_columns) == len(result_columns)
        for i, col in enumerate(record.result_columns):
            assert isinstance(col, Column)
            assert col.name == result_columns[i]["name"]
            assert col.type == record_data["result_columns"][i]["type"]
    elif expected_type == DataRecord:
        assert record.data == record_data["data"]
    elif expected_type == SuccessRecord:
        # Check that statistics dict has the expected values
        assert isinstance(record.statistics, Statistics)
        for key, value in record_data["statistics"].items():
            assert getattr(record.statistics, key) == value
    elif expected_type == ErrorRecord:
        assert record.errors == record_data["errors"]
        assert record.query_id == record_data["query_id"]
        assert record.query_label == record_data["query_label"]
        assert record.request_id == record_data["request_id"]
        # Check that statistics dict has the expected values
        assert isinstance(record.statistics, Statistics)
        for key, value in record_data["statistics"].items():
            assert getattr(record.statistics, key) == value


def test_parse_json_lines_record_invalid_message_type():
    """Test that parse_json_lines_record raises error for invalid message type."""
    with raises(ValueError):
        parse_json_lines_record({"message_type": "INVALID_TYPE"})


def test_parse_json_lines_record_invalid_format():
    """Test that parse_json_lines_record raises error for invalid record format."""
    with raises(OperationalError) as exc_info:
        # Missing required fields
        parse_json_lines_record({"message_type": "START"})

    assert "Invalid JSON lines record format" in str(exc_info.value)
