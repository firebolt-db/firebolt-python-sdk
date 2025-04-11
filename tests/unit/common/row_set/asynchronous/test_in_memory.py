import json
from unittest.mock import AsyncMock

import pytest
from httpx import Response

from firebolt.common.row_set.asynchronous.in_memory import InMemoryAsyncRowSet
from firebolt.utils.exception import DataError


class TestInMemoryAsyncRowSet:
    """Tests for InMemoryAsyncRowSet functionality."""

    @pytest.fixture
    def in_memory_rowset(self):
        """Create a fresh InMemoryAsyncRowSet instance."""
        return InMemoryAsyncRowSet()

    @pytest.fixture
    def mock_response(self):
        """Create a mock Response with valid JSON data."""
        mock = AsyncMock(spec=Response)

        # Create an async iterator for the aiter_bytes method
        async def mock_aiter_bytes():
            yield json.dumps(
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

        mock.aiter_bytes = mock_aiter_bytes
        return mock

    async def test_init(self, in_memory_rowset):
        """Test initialization state."""
        assert hasattr(in_memory_rowset, "_sync_row_set")

    async def test_append_empty_response(self, in_memory_rowset):
        """Test appending an empty response."""
        in_memory_rowset.append_empty_response()

        assert in_memory_rowset.row_count == -1
        assert in_memory_rowset.columns == []
        assert in_memory_rowset.statistics is None

    async def test_append_response(self, in_memory_rowset, mock_response):
        """Test appending a response with data."""
        await in_memory_rowset.append_response(mock_response)

        # Verify basic properties
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
        mock_response.aclose.assert_awaited_once()

    async def test_append_response_empty_content(self, in_memory_rowset):
        """Test appending a response with empty content."""
        mock = AsyncMock(spec=Response)

        async def mock_empty_aiter_bytes():
            yield b""

        mock.aiter_bytes = mock_empty_aiter_bytes

        await in_memory_rowset.append_response(mock)

        assert in_memory_rowset.row_count == -1
        assert in_memory_rowset.columns == []

        # Verify response is closed
        mock.aclose.assert_awaited_once()

    async def test_append_response_invalid_json(self, in_memory_rowset):
        """Test appending a response with invalid JSON."""
        mock = AsyncMock(spec=Response)

        async def mock_invalid_json_aiter_bytes():
            yield b"{invalid json}"

        mock.aiter_bytes = mock_invalid_json_aiter_bytes

        with pytest.raises(DataError) as err:
            await in_memory_rowset.append_response(mock)

        assert "Invalid query data format" in str(err.value)

        # Verify response is closed even if there's an error
        mock.aclose.assert_awaited_once()

    async def test_append_response_missing_meta(self, in_memory_rowset):
        """Test appending a response with missing meta field."""
        mock = AsyncMock(spec=Response)

        async def mock_missing_meta_aiter_bytes():
            yield json.dumps(
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

        mock.aiter_bytes = mock_missing_meta_aiter_bytes

        with pytest.raises(DataError) as err:
            await in_memory_rowset.append_response(mock)

        assert "Invalid query data format" in str(err.value)

        # Verify response is closed even if there's an error
        mock.aclose.assert_awaited_once()

    async def test_append_response_missing_data(self, in_memory_rowset):
        """Test appending a response with missing data field."""
        mock = AsyncMock(spec=Response)

        async def mock_missing_data_aiter_bytes():
            yield json.dumps(
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

        mock.aiter_bytes = mock_missing_data_aiter_bytes

        with pytest.raises(DataError) as err:
            await in_memory_rowset.append_response(mock)

        assert "Invalid query data format" in str(err.value)

        # Verify response is closed even if there's an error
        mock.aclose.assert_awaited_once()

    async def test_nextset_no_more_sets(self, in_memory_rowset, mock_response):
        """Test nextset when there are no more result sets."""
        await in_memory_rowset.append_response(mock_response)

        assert await in_memory_rowset.nextset() is False

    async def test_nextset_with_more_sets(self, in_memory_rowset, mock_response):
        """Test nextset when there are more result sets."""
        # Add two result sets
        await in_memory_rowset.append_response(mock_response)

        second_mock = AsyncMock(spec=Response)

        async def mock_second_aiter_bytes():
            yield json.dumps(
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

        second_mock.aiter_bytes = mock_second_aiter_bytes
        await in_memory_rowset.append_response(second_mock)

        # Verify first result set
        assert in_memory_rowset.columns[0].name == "col1"

        # Move to next result set
        assert await in_memory_rowset.nextset() is True

        # Verify second result set
        assert in_memory_rowset.columns[0].name == "col3"

        # No more result sets
        assert await in_memory_rowset.nextset() is False

    async def test_nextset_resets_current_row(self, in_memory_rowset, mock_response):
        """Test that nextset resets the current row index."""
        await in_memory_rowset.append_response(mock_response)

        # Add second result set
        second_mock = AsyncMock(spec=Response)

        async def mock_second_aiter_bytes():
            yield json.dumps(
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

        second_mock.aiter_bytes = mock_second_aiter_bytes
        await in_memory_rowset.append_response(second_mock)

        # Get a row from the first result set
        await in_memory_rowset.__anext__()

        # Move to next result set
        await in_memory_rowset.nextset()

        # First row of second result set should be accessible
        row = await in_memory_rowset.__anext__()
        assert row == [3.14]

    async def test_iteration(self, in_memory_rowset, mock_response):
        """Test async row iteration."""
        await in_memory_rowset.append_response(mock_response)

        rows = []
        async for row in in_memory_rowset:
            rows.append(row)

        assert len(rows) == 2
        assert rows[0] == [1, "one"]
        assert rows[1] == [2, "two"]

        # Iteration past the end should raise StopAsyncIteration
        with pytest.raises(StopAsyncIteration):
            await in_memory_rowset.__anext__()

    async def test_iteration_after_nextset(self, in_memory_rowset, mock_response):
        """Test row iteration after calling nextset."""
        await in_memory_rowset.append_response(mock_response)

        # Add second result set
        second_mock = AsyncMock(spec=Response)

        async def mock_second_aiter_bytes():
            yield json.dumps(
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

        second_mock.aiter_bytes = mock_second_aiter_bytes
        await in_memory_rowset.append_response(second_mock)

        # Fetch one row from first result set
        row = await in_memory_rowset.__anext__()
        assert row == [1, "one"]

        # Move to next result set
        await in_memory_rowset.nextset()

        # Verify we can iterate over second result set
        rows = []
        async for row in in_memory_rowset:
            rows.append(row)

        assert len(rows) == 2
        assert rows[0] == [3.14]
        assert rows[1] == [2.71]

    async def test_empty_rowset(self, in_memory_rowset):
        """Test __anext__ on an empty row set."""
        in_memory_rowset.append_empty_response()

        with pytest.raises(DataError) as err:
            await in_memory_rowset.__anext__()

        assert "no rows to fetch" in str(err.value)

    async def test_aclose(self, in_memory_rowset, mock_response):
        """Test aclose method."""
        await in_memory_rowset.append_response(mock_response)

        # Verify we can access data before closing
        assert in_memory_rowset.row_count == 2

        # Close the row set
        await in_memory_rowset.aclose()

        # Verify we can still access data after closing
        assert in_memory_rowset.row_count == 2

        # Verify we can still iterate after closing
        rows = []
        async for row in in_memory_rowset:
            rows.append(row)

        assert len(rows) == 2
