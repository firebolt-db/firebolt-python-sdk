import json
from typing import List
from unittest.mock import MagicMock, patch

import pytest
from httpx import Response
from pytest import raises

from firebolt.common._types import ColType
from firebolt.common.row_set.json_lines import Column as JSONColumn
from firebolt.common.row_set.json_lines import (
    DataRecord,
    ErrorRecord,
    MessageType,
    StartRecord,
    SuccessRecord,
)
from firebolt.common.row_set.streaming_common import StreamingRowSetCommonBase
from firebolt.common.row_set.types import Statistics
from firebolt.utils.exception import DataError, OperationalError


class TestStreamingRowSetCommon(StreamingRowSetCommonBase):
    """Test implementation of StreamingRowSetCommonBase."""

    def __init__(self) -> None:
        """Initialize the test class with required attributes."""
        super().__init__()
        # Initialize _rows_returned for tests
        self._rows_returned = 0

    def _parse_row(self, row_data) -> List[ColType]:
        """Concrete implementation of _parse_row for testing."""
        return row_data


class TestStreamingRowSetCommonBase:
    """Tests for StreamingRowSetCommonBase."""

    @pytest.fixture
    def streaming_rowset(self):
        """Create a TestStreamingRowSetCommon instance."""
        return TestStreamingRowSetCommon()

    def test_init(self, streaming_rowset):
        """Test initialization."""
        assert streaming_rowset._responses == []
        assert streaming_rowset._current_row_set_idx == 0

        # These should be reset
        assert hasattr(streaming_rowset, "_rows_returned")
        assert streaming_rowset._current_row_count == -1
        assert streaming_rowset._current_statistics is None
        assert streaming_rowset._current_columns is None
        assert streaming_rowset._response_consumed is False

        assert streaming_rowset._current_record is None
        assert streaming_rowset._current_record_row_idx == -1

    def test_reset(self, streaming_rowset):
        """Test _reset method."""
        # Set some values
        streaming_rowset._current_row_set_idx = 11
        streaming_rowset._current_row_count = 10
        streaming_rowset._current_statistics = MagicMock()
        streaming_rowset._current_record = MagicMock()
        streaming_rowset._current_record_row_idx = 5
        streaming_rowset._response_consumed = True
        streaming_rowset._current_columns = [MagicMock()]

        # Reset
        streaming_rowset._reset()

        # Check values are reset
        assert streaming_rowset._current_row_set_idx == 11
        assert streaming_rowset._current_row_count == -1
        assert streaming_rowset._current_statistics is None
        assert streaming_rowset._current_record is None
        assert streaming_rowset._current_record_row_idx == -1
        assert streaming_rowset._response_consumed is False
        assert streaming_rowset._current_columns is None

    def test_current_response(self, streaming_rowset):
        """Test _current_response property."""
        # No responses
        with raises(DataError) as exc_info:
            streaming_rowset._current_response
        assert "No results available" in str(exc_info.value)

        # Add a response
        mock_response = MagicMock(spec=Response)
        streaming_rowset._responses.append(mock_response)

        # Make sure row_set_idx is at the correct position
        streaming_rowset._current_row_set_idx = 0

        assert streaming_rowset._current_response == mock_response

    def test_next_json_lines_record_from_line_none(self, streaming_rowset):
        """Test _next_json_lines_record_from_line with None line."""
        assert streaming_rowset._next_json_lines_record_from_line(None) is None

    def test_next_json_lines_record_from_line_invalid_json(self, streaming_rowset):
        """Test _next_json_lines_record_from_line with invalid JSON."""
        with raises(OperationalError) as exc_info:
            streaming_rowset._next_json_lines_record_from_line("invalid json")
        assert "Invalid JSON line response format" in str(exc_info.value)

    @patch("firebolt.common.row_set.streaming_common.parse_json_lines_record")
    def test_next_json_lines_record_from_line_start(self, mock_parse, streaming_rowset):
        """Test _next_json_lines_record_from_line with START record."""
        # Create a mock record that will be returned by parse_json_lines_record
        mock_record = MagicMock(spec=StartRecord)
        mock_record.message_type = MessageType.start
        mock_parse.return_value = mock_record

        start_record_json = {
            "message_type": "START",
            "result_columns": [{"name": "col1", "type": "int"}],
            "query_id": "query_id",
            "query_label": "query_label",
            "request_id": "request_id",
        }

        result = streaming_rowset._next_json_lines_record_from_line(
            json.dumps(start_record_json)
        )
        assert result == mock_record
        assert result.message_type == MessageType.start
        mock_parse.assert_called_once()

    @patch("firebolt.common.row_set.streaming_common.parse_json_lines_record")
    def test_next_json_lines_record_from_line_data(self, mock_parse, streaming_rowset):
        """Test _next_json_lines_record_from_line with DATA record."""
        # Create a mock record that will be returned by parse_json_lines_record
        mock_record = MagicMock(spec=DataRecord)
        mock_record.message_type = MessageType.data
        mock_record.data = [[1, 2, 3]]
        mock_parse.return_value = mock_record

        data_record_json = {
            "message_type": "DATA",
            "data": [[1, 2, 3]],
        }

        result = streaming_rowset._next_json_lines_record_from_line(
            json.dumps(data_record_json)
        )
        assert result == mock_record
        assert result.message_type == MessageType.data
        assert result.data == [[1, 2, 3]]
        mock_parse.assert_called_once()

    @patch("firebolt.common.row_set.streaming_common.parse_json_lines_record")
    def test_next_json_lines_record_from_line_error(self, mock_parse, streaming_rowset):
        """Test _next_json_lines_record_from_line with ERROR record."""
        # Create a mock record that will be returned by parse_json_lines_record
        mock_record = MagicMock(spec=ErrorRecord)
        mock_record.message_type = MessageType.error
        mock_record.errors = [{"msg": "error message", "error_code": 123}]
        stats = Statistics(
            elapsed=0.1,
            rows_read=10,
            bytes_read=100,
            time_before_execution=0.01,
            time_to_execute=0.09,
        )
        mock_record.statistics = stats
        mock_parse.return_value = mock_record

        error_record_json = {
            "message_type": "FINISH_WITH_ERROR",
            "errors": [{"msg": "error message", "error_code": 123}],
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
        }

        with patch(
            "firebolt.common.row_set.streaming_common.FireboltStructuredError"
        ) as mock_error:
            with raises(Exception):
                streaming_rowset._next_json_lines_record_from_line(
                    json.dumps(error_record_json)
                )

        assert streaming_rowset._response_consumed is True
        assert streaming_rowset._current_statistics == stats
        mock_parse.assert_called_once()
        mock_error.assert_called_once()

    def test_fetch_columns_from_record_none(self, streaming_rowset):
        """Test _fetch_columns_from_record with None record."""
        with raises(OperationalError) as exc_info:
            streaming_rowset._fetch_columns_from_record(None)

        assert "Unexpected end of response stream" in str(exc_info.value)
        assert streaming_rowset._response_consumed is True

    def test_fetch_columns_from_record_wrong_type(self, streaming_rowset):
        """Test _fetch_columns_from_record with wrong record type."""
        # Create a mock data record with proper message_type enum
        data_record = MagicMock(spec=DataRecord)
        data_record.message_type = MessageType.data

        with raises(OperationalError) as exc_info:
            streaming_rowset._fetch_columns_from_record(data_record)

        assert "Unexpected json line message type" in str(exc_info.value)
        assert streaming_rowset._response_consumed is True

    def test_fetch_columns_from_record(self, streaming_rowset):
        """Test _fetch_columns_from_record with valid record."""
        # Create proper columns and start record with message_type as enum
        columns = [
            JSONColumn(name="col1", type="int"),
            JSONColumn(name="col2", type="string"),
        ]
        start_record = MagicMock(spec=StartRecord)
        start_record.message_type = MessageType.start
        start_record.result_columns = columns

        with patch(
            "firebolt.common.row_set.streaming_common.parse_type",
            side_effect=[int, str],
        ) as mock_parse_type:
            result = streaming_rowset._fetch_columns_from_record(start_record)

        assert len(result) == 2
        assert result[0].name == "col1"
        assert result[0].type_code == int
        assert result[1].name == "col2"
        assert result[1].type_code == str

    def test_pop_data_record_from_record_none_consumed(self, streaming_rowset):
        """Test _pop_data_record_from_record with None and consumed response."""
        streaming_rowset._response_consumed = True
        assert streaming_rowset._pop_data_record_from_record(None) is None

    def test_pop_data_record_from_record_none_not_consumed(self, streaming_rowset):
        """Test _pop_data_record_from_record with None and not consumed response."""
        streaming_rowset._response_consumed = False

        with raises(OperationalError) as exc_info:
            streaming_rowset._pop_data_record_from_record(None)

        assert "Unexpected end of response stream" in str(exc_info.value)
        assert streaming_rowset._response_consumed is True

    def test_pop_data_record_from_record_success(self, streaming_rowset):
        """Test _pop_data_record_from_record with SuccessRecord."""
        streaming_rowset._rows_returned = 10

        stats = Statistics(
            elapsed=0.1,
            rows_read=10,
            bytes_read=100,
            time_before_execution=0.01,
            time_to_execute=0.09,
        )

        # Create success record with message_type as enum
        success_record = MagicMock(spec=SuccessRecord)
        success_record.message_type = MessageType.success
        success_record.statistics = stats

        assert streaming_rowset._pop_data_record_from_record(success_record) is None
        assert streaming_rowset._current_row_count == 10
        assert streaming_rowset._current_statistics == stats
        assert streaming_rowset._response_consumed is True

    def test_pop_data_record_from_record_wrong_type(self, streaming_rowset):
        """Test _pop_data_record_from_record with wrong record type."""
        # Create start record with message_type as enum
        start_record = MagicMock(spec=StartRecord)
        start_record.message_type = MessageType.start

        with raises(OperationalError) as exc_info:
            streaming_rowset._pop_data_record_from_record(start_record)

        assert "Unexpected json line message type" in str(exc_info.value)

    def test_pop_data_record_from_record_data(self, streaming_rowset):
        """Test _pop_data_record_from_record with DataRecord."""
        # Create data record with message_type as enum
        data_record = MagicMock(spec=DataRecord)
        data_record.message_type = MessageType.data
        data_record.data = [[1, 2, 3]]

        result = streaming_rowset._pop_data_record_from_record(data_record)

        assert result == data_record

    def test_get_next_data_row_from_current_record_none(self, streaming_rowset):
        """Test _get_next_data_row_from_current_record with None record."""
        streaming_rowset._current_record = None

        with raises(StopIteration):
            streaming_rowset._get_next_data_row_from_current_record()

    def test_get_next_data_row_from_current_record(self, streaming_rowset):
        """Test _get_next_data_row_from_current_record with valid record."""
        streaming_rowset._rows_returned = 0

        # Create a proper data record
        data_record = MagicMock(spec=DataRecord)
        data_record.data = [[1, 2, 3], [4, 5, 6]]
        streaming_rowset._current_record = data_record
        streaming_rowset._current_record_row_idx = 0

        row = streaming_rowset._get_next_data_row_from_current_record()

        assert row == [1, 2, 3]
        assert streaming_rowset._current_record_row_idx == 1
        assert streaming_rowset._rows_returned == 1

        row = streaming_rowset._get_next_data_row_from_current_record()

        assert row == [4, 5, 6]
        assert streaming_rowset._current_record_row_idx == 2
        assert streaming_rowset._rows_returned == 2
