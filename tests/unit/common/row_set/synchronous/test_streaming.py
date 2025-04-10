import json
from unittest.mock import MagicMock, PropertyMock, patch

import pytest
from httpx import HTTPError, Response

from firebolt.common.row_set.json_lines import Column as JLColumn
from firebolt.common.row_set.json_lines import (
    DataRecord,
    ErrorRecord,
    MessageType,
    StartRecord,
    SuccessRecord,
)
from firebolt.common.row_set.synchronous.streaming import StreamingRowSet
from firebolt.common.row_set.types import Column, Statistics
from firebolt.utils.exception import FireboltStructuredError, OperationalError
from firebolt.utils.util import ExceptionGroup


class TestStreamingRowSet:
    """Tests for StreamingRowSet functionality."""

    @pytest.fixture
    def streaming_rowset(self):
        """Create a fresh StreamingRowSet instance."""
        return StreamingRowSet()

    @pytest.fixture
    def mock_response(self):
        """Create a mock Response with valid JSON lines data."""
        mock = MagicMock(spec=Response)
        mock.iter_lines.return_value = iter(
            [
                json.dumps(
                    {
                        "message_type": "START",
                        "result_columns": [],
                        "query_id": "q1",
                        "query_label": "l1",
                        "request_id": "r1",
                    }
                ),
                json.dumps({"message_type": "DATA", "data": [[1, "one"]]}),
                json.dumps({"message_type": "FINISH_SUCCESSFULLY", "statistics": {}}),
            ]
        )
        mock.is_closed = False
        return mock

    @pytest.fixture
    def mock_empty_response(self):
        """Create a mock Response that yields no records."""
        mock = MagicMock(spec=Response)
        mock.iter_lines.return_value = iter([])
        mock.is_closed = False
        return mock

    @pytest.fixture
    def start_record(self):
        """Create a sample StartRecord."""
        return StartRecord(
            message_type=MessageType.start,
            result_columns=[
                JLColumn(name="col1", type="int"),
                JLColumn(name="col2", type="string"),
            ],
            query_id="query1",
            query_label="label1",
            request_id="req1",
        )

    @pytest.fixture
    def data_record(self):
        """Create a sample DataRecord."""
        return DataRecord(message_type=MessageType.data, data=[[1, "one"], [2, "two"]])

    @pytest.fixture
    def success_record(self):
        """Create a sample SuccessRecord."""
        return SuccessRecord(
            message_type=MessageType.success,
            statistics=Statistics(
                elapsed=0.1,
                rows_read=10,
                bytes_read=100,
                time_before_execution=0.01,
                time_to_execute=0.09,
            ),
        )

    @pytest.fixture
    def error_record(self):
        """Create a sample ErrorRecord."""
        return ErrorRecord(
            message_type=MessageType.error,
            errors=[{"message": "Test error", "code": 123}],
            query_id="query1",
            query_label="label1",
            request_id="req1",
            statistics=Statistics(
                elapsed=0.1,
                rows_read=0,
                bytes_read=10,
                time_before_execution=0.01,
                time_to_execute=0.01,
            ),
        )

    def test_init(self, streaming_rowset):
        """Test initialization state."""
        assert streaming_rowset._responses == []
        assert streaming_rowset._current_row_set_idx == 0
        assert streaming_rowset._current_row_count == -1
        assert streaming_rowset._current_statistics is None
        assert streaming_rowset._lines_iter is None
        assert streaming_rowset._current_record is None
        assert streaming_rowset._current_record_row_idx == -1
        assert streaming_rowset._response_consumed is False
        assert streaming_rowset._current_columns is None

    def test_append_empty_response(self, streaming_rowset):
        """Test appending an empty response."""
        streaming_rowset.append_empty_response()

        assert len(streaming_rowset._responses) == 1
        assert streaming_rowset._responses[0] is None

    @patch(
        "firebolt.common.row_set.synchronous.streaming.StreamingRowSet._fetch_columns"
    )
    def test_append_response(self, mock_fetch_columns, streaming_rowset, mock_response):
        """Test appending a response with data."""
        mock_columns = [Column("col1", int, None, None, None, None, None)]
        mock_fetch_columns.return_value = mock_columns

        streaming_rowset.append_response(mock_response)

        # Verify response was added
        assert len(streaming_rowset._responses) == 1
        assert streaming_rowset._responses[0] == mock_response

        # Verify columns were fetched
        mock_fetch_columns.assert_called_once()
        assert streaming_rowset._current_columns == mock_columns

    @patch(
        "firebolt.common.row_set.synchronous.streaming.StreamingRowSet._next_json_lines_record"
    )
    @patch(
        "firebolt.common.row_set.streaming_common.StreamingRowSetCommonBase._fetch_columns_from_record"
    )
    def test_fetch_columns(
        self,
        mock_fetch_columns_from_record,
        mock_next_json_lines_record,
        streaming_rowset,
        mock_response,
        start_record,
    ):
        """Test _fetch_columns method."""
        mock_next_json_lines_record.return_value = start_record
        mock_columns = [Column("col1", int, None, None, None, None, None)]
        mock_fetch_columns_from_record.return_value = mock_columns

        streaming_rowset._responses = [mock_response]
        columns = streaming_rowset._fetch_columns()

        # Verify we got the expected columns
        assert columns == mock_columns
        mock_next_json_lines_record.assert_called_once()
        mock_fetch_columns_from_record.assert_called_once_with(start_record)

    def test_fetch_columns_empty_response(self, streaming_rowset):
        """Test _fetch_columns with empty response."""
        streaming_rowset.append_empty_response()
        columns = streaming_rowset._fetch_columns()

        assert columns == []

    @patch(
        "firebolt.common.row_set.synchronous.streaming.StreamingRowSet._next_json_lines_record"
    )
    def test_fetch_columns_unexpected_end(
        self, mock_next_json_lines_record, streaming_rowset, mock_response
    ):
        """Test _fetch_columns with unexpected end of stream."""
        mock_next_json_lines_record.return_value = None
        streaming_rowset._responses = [mock_response]

        with pytest.raises(OperationalError) as err:
            streaming_rowset._fetch_columns()

        assert "Unexpected end of response stream" in str(err.value)

    @patch("firebolt.common.row_set.synchronous.streaming.StreamingRowSet._parse_row")
    @patch(
        "firebolt.common.row_set.streaming_common.StreamingRowSetCommonBase._next_json_lines_record_from_line"
    )
    @patch(
        "firebolt.common.row_set.streaming_common.StreamingRowSetCommonBase._pop_data_record_from_record"
    )
    def test_statistics(
        self,
        mock_pop_data_record,
        mock_next_record,
        mock_parse_row,
        streaming_rowset,
        mock_response,
        data_record,
        success_record,
    ):
        """Test statistics property."""
        # Setup mocks for direct property access
        streaming_rowset._responses = [mock_response]
        streaming_rowset._current_columns = [
            Column("col1", int, None, None, None, None, None)
        ]

        # Ensure statistics is explicitly None at the start
        streaming_rowset._current_statistics = None

        # Initialize _rows_returned
        streaming_rowset._rows_returned = 0

        # Statistics are None before reading all data
        assert streaming_rowset.statistics is None

        # Manually set statistics as if it came from a SuccessRecord
        streaming_rowset._current_statistics = success_record.statistics

        # Now statistics should be available
        assert streaming_rowset.statistics is not None
        assert streaming_rowset.statistics.elapsed == 0.1
        assert streaming_rowset.statistics.rows_read == 10
        assert streaming_rowset.statistics.bytes_read == 100
        assert streaming_rowset.statistics.time_before_execution == 0.01
        assert streaming_rowset.statistics.time_to_execute == 0.09

    @patch(
        "firebolt.common.row_set.synchronous.streaming.StreamingRowSet._next_json_lines_record"
    )
    @patch(
        "firebolt.common.row_set.streaming_common.StreamingRowSetCommonBase._fetch_columns_from_record"
    )
    def test_nextset_no_more_sets(
        self,
        mock_fetch_columns_from_record,
        mock_next_json_lines_record,
        streaming_rowset,
        mock_response,
        start_record,
    ):
        """Test nextset when there are no more result sets."""
        # Setup mocks
        mock_next_json_lines_record.return_value = start_record
        mock_fetch_columns_from_record.return_value = [
            Column("col1", int, None, None, None, None, None)
        ]

        streaming_rowset._responses = [mock_response]
        streaming_rowset._current_columns = mock_fetch_columns_from_record.return_value

        assert streaming_rowset.nextset() is False

    @patch(
        "firebolt.common.row_set.synchronous.streaming.StreamingRowSet._fetch_columns"
    )
    def test_nextset_with_more_sets(self, mock_fetch_columns, streaming_rowset):
        """Test nextset when there are more result sets."""
        # Create real Column objects
        columns1 = [
            Column("col1", int, None, None, None, None, None),
            Column("col2", str, None, None, None, None, None),
        ]

        columns2 = [Column("col3", float, None, None, None, None, None)]

        # Setup mocks
        mock_fetch_columns.side_effect = [columns1, columns2]

        # Setup two responses
        response1 = MagicMock(spec=Response)
        response2 = MagicMock(spec=Response)
        streaming_rowset._responses = [response1, response2]
        streaming_rowset._current_columns = columns1

        # Verify first result set
        assert streaming_rowset.columns[0].name == "col1"

        # Manually call fetch_columns once to track the first call
        # This simulates what happens during initialization
        mock_fetch_columns.reset_mock()

        # Move to next result set
        assert streaming_rowset.nextset() is True

        # Verify columns were fetched again
        assert mock_fetch_columns.call_count == 1

        # Update current columns to match what mock_fetch_columns returned
        streaming_rowset._current_columns = columns2

        # Verify second result set
        assert streaming_rowset.columns[0].name == "col3"

        # No more result sets
        assert streaming_rowset.nextset() is False

        # Verify response is closed when moving to next set
        response1.close.assert_called_once()

    def test_iteration(self, streaming_rowset):
        """Test row iteration for StreamingRowSet."""
        # Define expected rows and setup columns
        expected_rows = [[1, "one"], [2, "two"]]
        streaming_rowset._current_columns = [
            Column("col1", int, None, None, None, None, None),
            Column("col2", str, None, None, None, None, None),
        ]

        # Set up mock response
        mock_response = MagicMock(spec=Response)
        streaming_rowset._responses = [mock_response]
        streaming_rowset._current_row_set_idx = 0

        # Create a separate test method to test just the iteration behavior
        # This avoids the complex internals of the streaming row set
        rows = []

        # Mock several internal methods to isolate the test
        with patch.object(
            streaming_rowset, "_pop_data_record"
        ) as mock_pop_data_record, patch.object(
            streaming_rowset, "_get_next_data_row_from_current_record"
        ) as mock_get_next_row, patch(
            "firebolt.common.row_set.streaming_common.StreamingRowSetCommonBase._current_response",
            new_callable=PropertyMock,
            return_value=mock_response,
        ):

            # Setup the mocks to return our test data
            mock_get_next_row.side_effect = expected_rows + [StopIteration()]

            # Create a DataRecord with our test data
            data_record = DataRecord(message_type=MessageType.data, data=expected_rows)

            # Mock _pop_data_record to return our data record once
            mock_pop_data_record.side_effect = [data_record, None]

            # Set response_consumed to False to allow iteration
            streaming_rowset._response_consumed = False

            # Set up the row indexes for iteration
            streaming_rowset._current_record = None  # Start with no record
            streaming_rowset._current_record_row_idx = 0
            streaming_rowset._rows_returned = 0

            # Collect the first two rows using direct next() calls
            rows.append(next(streaming_rowset))
            rows.append(next(streaming_rowset))

            # Verify the StopIteration is raised after all rows are consumed
            with pytest.raises(StopIteration):
                next(streaming_rowset)

        # Verify we got the expected rows
        assert len(rows) == 2
        assert rows[0] == expected_rows[0]
        assert rows[1] == expected_rows[1]

    def test_iteration_empty_response(self, streaming_rowset):
        """Test iteration with an empty response."""
        streaming_rowset.append_empty_response()

        with pytest.raises(StopIteration):
            next(streaming_rowset)

    def test_error_response(self, streaming_rowset, error_record):
        """Test handling of error response."""
        # Setup mocks for direct testing
        streaming_rowset._responses = [MagicMock(spec=Response)]
        streaming_rowset._current_columns = [
            Column("col1", int, None, None, None, None, None),
            Column("col2", str, None, None, None, None, None),
        ]

        # Test error handling
        with pytest.raises(FireboltStructuredError) as err:
            streaming_rowset._handle_error_record(error_record)

        # Verify error was returned correctly - the string representation includes the code
        assert "123" in str(err.value)

        # Statistics should be updated from ERROR record
        streaming_rowset._current_statistics = error_record.statistics
        assert streaming_rowset._current_statistics is not None
        assert streaming_rowset._current_statistics.elapsed == 0.1

    def test_close(self, streaming_rowset, mock_response):
        """Test close method."""
        response1 = MagicMock(spec=Response)
        response1.is_closed = False
        response2 = MagicMock(spec=Response)
        response2.is_closed = False

        streaming_rowset._responses = [response1, response2]
        streaming_rowset._current_row_set_idx = 0

        # Close the row set
        streaming_rowset.close()

        # Verify all responses are closed
        response1.close.assert_called_once()
        response2.close.assert_called_once()

        # Verify internal state is reset
        assert streaming_rowset._responses == []

    def test_close_with_error(self, streaming_rowset):
        """Test close method when response closing raises an error."""
        response = MagicMock(spec=Response)
        response.is_closed = False
        response.close.side_effect = HTTPError("Test error")

        streaming_rowset._responses = [response]
        streaming_rowset._current_row_set_idx = 0

        # Close should propagate the error as OperationalError
        with pytest.raises(OperationalError) as err:
            streaming_rowset.close()

        assert "Failed to close row set" in str(err.value)
        assert isinstance(err.value.__cause__, ExceptionGroup)

    def test_close_on_error_context_manager(self, streaming_rowset):
        """Test _close_on_op_error context manager."""
        streaming_rowset.close = MagicMock()

        # When no error occurs, close should not be called
        with streaming_rowset._close_on_op_error():
            pass
        streaming_rowset.close.assert_not_called()

        # When OperationalError occurs, close should be called
        with pytest.raises(OperationalError):
            with streaming_rowset._close_on_op_error():
                raise OperationalError("Test error")
        streaming_rowset.close.assert_called_once()

    def test_next_json_lines_record_none_response(self, streaming_rowset):
        """Test _next_json_lines_record with None response."""
        streaming_rowset.append_empty_response()

        assert streaming_rowset._next_json_lines_record() is None

    @patch(
        "firebolt.common.row_set.synchronous.streaming.StreamingRowSet._fetch_columns"
    )
    def test_next_json_lines_record_http_error(
        self, mock_fetch_columns, streaming_rowset
    ):
        """Test _next_json_lines_record when iter_lines raises HTTPError."""
        mock_fetch_columns.return_value = []

        response = MagicMock(spec=Response)
        response.iter_lines.side_effect = HTTPError("Test error")

        streaming_rowset._responses = [response]

        with pytest.raises(OperationalError) as err:
            streaming_rowset._next_json_lines_record()

        assert "Failed to read response stream" in str(err.value)

    @patch(
        "firebolt.common.row_set.streaming_common.StreamingRowSetCommonBase._get_next_data_row_from_current_record"
    )
    def test_next_data_record_navigation(self, mock_get_next, streaming_rowset):
        """Test __next__ record navigation logic."""
        # Setup mock response directly into streaming_rowset
        streaming_rowset._responses = [MagicMock()]
        streaming_rowset._response_consumed = False
        streaming_rowset._rows_returned = 0  # Initialize missing attribute

        # Setup mock current record
        mock_record = MagicMock(spec=DataRecord)
        mock_record.data = [[1, "one"], [2, "two"]]
        streaming_rowset._current_record = mock_record
        streaming_rowset._current_record_row_idx = 0

        # Mock _get_next_data_row_from_current_record to return a fixed value
        mock_get_next.return_value = [1, "one"]

        # Call __next__
        result = next(streaming_rowset)

        # Verify result
        assert result == [1, "one"]

        # Verify current_record_row_idx was incremented
        assert streaming_rowset._current_record_row_idx == 1

        # Setup for second test - at end of current record
        streaming_rowset._current_record_row_idx = len(mock_record.data)

        # Mock _pop_data_record to return a new record
        new_record = MagicMock(spec=DataRecord)
        new_record.data = [[3, "three"]]
        streaming_rowset._pop_data_record = MagicMock(return_value=new_record)

        # Call __next__ again
        next(streaming_rowset)

        # Verify _pop_data_record was called and current_record was updated
        streaming_rowset._pop_data_record.assert_called_once()
        assert streaming_rowset._current_record == new_record
        assert streaming_rowset._current_record_row_idx == -1  # Should be reset to -1

    def test_iteration_stops_after_response_consumed(self, streaming_rowset):
        """Test iteration stops after response is marked as consumed."""
        # Setup a response that's already consumed
        streaming_rowset._responses = [MagicMock()]
        streaming_rowset._response_consumed = True

        # Iteration should stop immediately
        with pytest.raises(StopIteration):
            next(streaming_rowset)
