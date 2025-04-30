import json
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from httpx import Response

from firebolt.common.row_set.synchronous.in_memory import InMemoryRowSet
from firebolt.utils.exception import DataError


class TestInMemoryRowSet:
    """Tests for InMemoryRowSet functionality."""

    @pytest.fixture
    def in_memory_rowset(self) -> InMemoryRowSet:
        """Create a fresh InMemoryRowSet instance."""
        return InMemoryRowSet()

    @pytest.fixture
    def mock_response(self):
        """Create a mock Response with valid JSON data."""
        mock = MagicMock(spec=Response)
        mock.iter_bytes.return_value = [
            json.dumps(
                {
                    "meta": [
                        {"name": "col1", "type": "int"},
                        {"name": "col2", "type": "string"},
                    ],
                    "data": [[1, "one"], [2, "two"]],
                    "statistics": {
                        "elapsed": 0.1,
                        "rows_read": 10,
                        "bytes_read": 100,
                        "time_before_execution": 0.01,
                        "time_to_execute": 0.09,
                    },
                }
            ).encode("utf-8")
        ]
        return mock

    @pytest.fixture
    def mock_empty_bytes_stream(self):
        """Create a mock bytes stream with no content."""
        return iter([b""])

    @pytest.fixture
    def mock_bytes_stream(self):
        """Create a mock bytes stream with valid JSON data."""
        return iter(
            [
                json.dumps(
                    {
                        "meta": [
                            {"name": "col1", "type": "int"},
                            {"name": "col2", "type": "string"},
                        ],
                        "data": [[1, "one"], [2, "two"]],
                        "statistics": {
                            "elapsed": 0.1,
                            "rows_read": 10,
                            "bytes_read": 100,
                            "time_before_execution": 0.01,
                            "time_to_execute": 0.09,
                        },
                    }
                ).encode("utf-8")
            ]
        )

    @pytest.fixture
    def mock_multi_chunk_bytes_stream(self):
        """Create a mock bytes stream with valid JSON data split across multiple chunks."""
        part1 = json.dumps(
            {
                "meta": [
                    {"name": "col1", "type": "int"},
                    {"name": "col2", "type": "string"},
                ],
                "data": [[1, "one"], [2, "two"]],
                "statistics": {
                    "elapsed": 0.1,
                    "rows_read": 10,
                    "bytes_read": 100,
                    "time_before_execution": 0.01,
                    "time_to_execute": 0.09,
                },
            }
        )

        # Split into multiple chunks
        chunk_size = len(part1) // 3
        return iter(
            [
                part1[:chunk_size].encode("utf-8"),
                part1[chunk_size : 2 * chunk_size].encode("utf-8"),
                part1[2 * chunk_size :].encode("utf-8"),
            ]
        )

    def test_init(self, in_memory_rowset):
        """Test initialization state."""
        assert in_memory_rowset._row_sets == []
        assert in_memory_rowset._current_row_set_idx == 0
        assert in_memory_rowset._current_row == -1

    def test_append_empty_response(self, in_memory_rowset):
        """Test appending an empty response."""
        in_memory_rowset.append_empty_response()

        assert len(in_memory_rowset._row_sets) == 1
        assert in_memory_rowset.row_count == -1
        assert in_memory_rowset.columns == []
        assert in_memory_rowset.statistics is None

    def test_append_response(self, in_memory_rowset, mock_response):
        """Test appending a response with data."""
        in_memory_rowset.append_response(mock_response)

        # Verify basic properties
        assert len(in_memory_rowset._row_sets) == 1
        assert in_memory_rowset.row_count == 2
        assert len(in_memory_rowset.columns) == 2
        assert in_memory_rowset.statistics is not None

        # Verify columns
        assert in_memory_rowset.columns[0].name == "col1"
        assert in_memory_rowset.columns[0].type_code == int
        assert in_memory_rowset.columns[1].name == "col2"
        assert in_memory_rowset.columns[1].type_code == str

        # Verify statistics
        assert in_memory_rowset.statistics.elapsed == 0.1
        assert in_memory_rowset.statistics.rows_read == 10
        assert in_memory_rowset.statistics.bytes_read == 100
        assert in_memory_rowset.statistics.time_before_execution == 0.01
        assert in_memory_rowset.statistics.time_to_execute == 0.09

        # Verify response is closed
        mock_response.close.assert_called_once()

    def test_append_response_empty_content(self, in_memory_rowset):
        """Test appending a response with empty content."""
        mock = MagicMock(spec=Response)
        mock.iter_bytes.return_value = [b""]

        in_memory_rowset.append_response(mock)

        assert len(in_memory_rowset._row_sets) == 1
        assert in_memory_rowset.row_count == -1
        assert in_memory_rowset.columns == []

        # Verify response is closed
        mock.close.assert_called_once()

    def test_append_response_stream_empty(
        self, in_memory_rowset, mock_empty_bytes_stream
    ):
        """Test appending an empty stream."""
        in_memory_rowset.append_response_stream(mock_empty_bytes_stream)

        assert len(in_memory_rowset._row_sets) == 1
        assert in_memory_rowset.row_count == -1
        assert in_memory_rowset.columns == []
        assert in_memory_rowset.statistics is None

    def test_append_response_stream(self, in_memory_rowset, mock_bytes_stream):
        """Test appending a stream with data."""
        in_memory_rowset.append_response_stream(mock_bytes_stream)

        assert len(in_memory_rowset._row_sets) == 1
        assert in_memory_rowset.row_count == 2
        assert len(in_memory_rowset.columns) == 2
        assert in_memory_rowset.statistics is not None

    def test_append_response_stream_with_decimals(
        self,
        in_memory_rowset: InMemoryRowSet,
        mock_decimal_bytes_stream: Response,
    ):
        """Test appending a stream with decimal data type."""
        in_memory_rowset.append_response(mock_decimal_bytes_stream)

        assert len(in_memory_rowset._row_sets) == 1
        assert in_memory_rowset.row_count == 2
        assert len(in_memory_rowset.columns) == 3

        # Get the row values and check decimal values are equal
        rows = list(in_memory_rowset)

        # Verify the decimal value is correctly parsed
        for row in rows:
            assert isinstance(row[2], Decimal), "Expected Decimal type"
            assert (
                str(row[2]) == "1231232.123459999990457054844258706536"
            ), "Decimal value mismatch"

    def test_append_response_stream_multi_chunk(
        self, in_memory_rowset, mock_multi_chunk_bytes_stream
    ):
        """Test appending a multi-chunk stream."""
        in_memory_rowset.append_response_stream(mock_multi_chunk_bytes_stream)

        assert len(in_memory_rowset._row_sets) == 1
        assert in_memory_rowset.row_count == 2
        assert len(in_memory_rowset.columns) == 2
        assert in_memory_rowset.statistics is not None

    def test_append_response_invalid_json(self, in_memory_rowset):
        """Test appending a response with invalid JSON."""
        mock = MagicMock(spec=Response)
        mock.iter_bytes.return_value = [b"{invalid json}"]

        with pytest.raises(DataError) as err:
            in_memory_rowset.append_response(mock)

        assert "Invalid query data format" in str(err.value)

        # Verify response is closed even if there's an error
        mock.close.assert_called_once()

    def test_append_response_missing_meta(self, in_memory_rowset):
        """Test appending a response with missing meta field."""
        mock = MagicMock(spec=Response)
        mock.iter_bytes.return_value = [
            json.dumps(
                {
                    "data": [[1, "one"]],
                    "statistics": {
                        "elapsed": 0.1,
                        "rows_read": 10,
                        "bytes_read": 100,
                        "time_before_execution": 0.01,
                        "time_to_execute": 0.09,
                    },
                }
            ).encode("utf-8")
        ]

        with pytest.raises(DataError) as err:
            in_memory_rowset.append_response(mock)

        assert "Invalid query data format" in str(err.value)

        # Verify response is closed even if there's an error
        mock.close.assert_called_once()

    def test_append_response_missing_data(self, in_memory_rowset):
        """Test appending a response with missing data field."""
        mock = MagicMock(spec=Response)
        mock.iter_bytes.return_value = [
            json.dumps(
                {
                    "meta": [{"name": "col1", "type": "int"}],
                    "statistics": {
                        "elapsed": 0.1,
                        "rows_read": 10,
                        "bytes_read": 100,
                        "time_before_execution": 0.01,
                        "time_to_execute": 0.09,
                    },
                }
            ).encode("utf-8")
        ]

        with pytest.raises(DataError) as err:
            in_memory_rowset.append_response(mock)

        assert "Invalid query data format" in str(err.value)

        # Verify response is closed even if there's an error
        mock.close.assert_called_once()

    def test_row_set_property_no_results(self, in_memory_rowset):
        """Test _row_set property when no results are available."""
        with pytest.raises(DataError) as err:
            in_memory_rowset._row_set

        assert "No results available" in str(err.value)

    def test_row_set_property(self, in_memory_rowset, mock_response):
        """Test _row_set property returns the current row set."""
        in_memory_rowset.append_response(mock_response)

        row_set = in_memory_rowset._row_set
        assert row_set.row_count == 2
        assert len(row_set.columns) == 2
        assert row_set.statistics is not None

    def test_nextset_no_more_sets(self, in_memory_rowset, mock_response):
        """Test nextset when there are no more result sets."""
        in_memory_rowset.append_response(mock_response)

        assert in_memory_rowset.nextset() is False

    def test_nextset_with_more_sets(self, in_memory_rowset, mock_response):
        """Test nextset when there are more result sets."""
        # Add two result sets
        in_memory_rowset.append_response(mock_response)

        second_mock = MagicMock(spec=Response)
        second_mock.iter_bytes.return_value = [
            json.dumps(
                {
                    "meta": [{"name": "col3", "type": "float"}],
                    "data": [[3.14], [2.71]],
                    "statistics": {
                        "elapsed": 0.2,
                        "rows_read": 5,
                        "bytes_read": 50,
                        "time_before_execution": 0.02,
                        "time_to_execute": 0.18,
                    },
                }
            ).encode("utf-8")
        ]
        in_memory_rowset.append_response(second_mock)

        # Verify first result set
        assert in_memory_rowset.columns[0].name == "col1"

        # Move to next result set
        assert in_memory_rowset.nextset() is True

        # Verify second result set
        assert in_memory_rowset.columns[0].name == "col3"

        # No more result sets
        assert in_memory_rowset.nextset() is False

    def test_nextset_resets_current_row(self, in_memory_rowset, mock_response):
        """Test that nextset resets the current row index."""
        in_memory_rowset.append_response(mock_response)

        # Add second result set
        second_mock = MagicMock(spec=Response)
        second_mock.iter_bytes.return_value = [
            json.dumps(
                {
                    "meta": [{"name": "col3", "type": "float"}],
                    "data": [[3.14], [2.71]],
                    "statistics": {
                        "elapsed": 0.2,
                        "rows_read": 5,
                        "bytes_read": 50,
                        "time_before_execution": 0.02,
                        "time_to_execute": 0.18,
                    },
                }
            ).encode("utf-8")
        ]
        in_memory_rowset.append_response(second_mock)

        # Advance current row in first result set
        next(in_memory_rowset)
        assert in_memory_rowset._current_row == 0

        # Move to next result set
        in_memory_rowset.nextset()

        # Verify current row is reset
        assert in_memory_rowset._current_row == -1

    def test_iteration(self, in_memory_rowset, mock_response):
        """Test row iteration."""
        in_memory_rowset.append_response(mock_response)

        rows = list(in_memory_rowset)
        assert len(rows) == 2
        assert rows[0] == [1, "one"]
        assert rows[1] == [2, "two"]

        # Iteration past the end should raise StopIteration
        with pytest.raises(StopIteration):
            next(in_memory_rowset)

    def test_iteration_after_nextset(self, in_memory_rowset, mock_response):
        """Test row iteration after calling nextset."""
        in_memory_rowset.append_response(mock_response)

        # Add second result set
        second_mock = MagicMock(spec=Response)
        second_mock.iter_bytes.return_value = [
            json.dumps(
                {
                    "meta": [{"name": "col3", "type": "float"}],
                    "data": [[3.14], [2.71]],
                    "statistics": {
                        "elapsed": 0.2,
                        "rows_read": 5,
                        "bytes_read": 50,
                        "time_before_execution": 0.02,
                        "time_to_execute": 0.18,
                    },
                }
            ).encode("utf-8")
        ]
        in_memory_rowset.append_response(second_mock)

        # Fetch one row from first result set
        row = next(in_memory_rowset)
        assert row == [1, "one"]

        # Move to next result set
        in_memory_rowset.nextset()

        # Verify we can iterate over second result set
        rows = list(in_memory_rowset)
        assert len(rows) == 2
        assert rows[0] == [3.14]
        assert rows[1] == [2.71]

    def test_next_empty_rowset(self, in_memory_rowset):
        """Test __next__ on an empty row set."""
        in_memory_rowset.append_empty_response()

        with pytest.raises(DataError) as err:
            next(in_memory_rowset)

        assert "no rows to fetch" in str(err.value)

    def test_close(self, in_memory_rowset, mock_response):
        """Test close method (should be a no-op for InMemoryRowSet)."""
        in_memory_rowset.append_response(mock_response)

        # Verify we can access data before closing
        assert in_memory_rowset.row_count == 2

        # Close the row set
        in_memory_rowset.close()

        # Verify we can still access data after closing
        assert in_memory_rowset.row_count == 2

        # Verify we can still iterate after closing
        rows = list(in_memory_rowset)
        assert len(rows) == 2

    def test_parse_row(self, in_memory_rowset, mock_response):
        """Test _parse_row correctly transforms raw values to their Python types."""
        in_memory_rowset.append_response(mock_response)

        # Use _parse_row directly
        raw_row = [1, "one"]
        parsed_row = in_memory_rowset._parse_row(raw_row)

        assert isinstance(parsed_row[0], int)
        assert isinstance(parsed_row[1], str)
        assert parsed_row[0] == 1
        assert parsed_row[1] == "one"
