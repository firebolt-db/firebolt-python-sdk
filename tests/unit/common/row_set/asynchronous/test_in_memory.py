import json
from unittest.mock import MagicMock, patch

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
        """Create a mock async Response with valid JSON data."""
        mock = MagicMock(spec=Response)

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

        mock.aiter_bytes.return_value = mock_aiter_bytes()
        return mock

    @pytest.fixture
    def mock_empty_response(self):
        """Create a mock Response with empty content."""
        mock = MagicMock(spec=Response)

        async def mock_aiter_bytes():
            yield b""

        mock.aiter_bytes.return_value = mock_aiter_bytes()
        return mock

    @pytest.fixture
    def mock_invalid_json_response(self):
        """Create a mock Response with invalid JSON."""
        mock = MagicMock(spec=Response)

        async def mock_aiter_bytes():
            yield b"{invalid json}"

        mock.aiter_bytes.return_value = mock_aiter_bytes()
        return mock

    @pytest.fixture
    def mock_missing_meta_response(self):
        """Create a mock Response with missing meta field."""
        mock = MagicMock(spec=Response)

        async def mock_aiter_bytes():
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

        mock.aiter_bytes.return_value = mock_aiter_bytes()
        return mock

    @pytest.fixture
    def mock_missing_data_response(self):
        """Create a mock Response with missing data field."""
        mock = MagicMock(spec=Response)

        async def mock_aiter_bytes():
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

        mock.aiter_bytes.return_value = mock_aiter_bytes()
        return mock

    @pytest.fixture
    def mock_multi_chunk_response(self):
        """Create a mock Response with multi-chunk data."""
        mock = MagicMock(spec=Response)

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

        async def mock_aiter_bytes():
            yield part1[:chunk_size].encode("utf-8")
            yield part1[chunk_size : 2 * chunk_size].encode("utf-8")
            yield part1[2 * chunk_size :].encode("utf-8")

        mock.aiter_bytes.return_value = mock_aiter_bytes()
        return mock

    async def test_init(self, in_memory_rowset):
        """Test initialization state."""
        assert hasattr(in_memory_rowset, "_sync_row_set")

        # With no data, the properties will throw DataError
        with pytest.raises(DataError) as err:
            in_memory_rowset.columns
        assert "No results available" in str(err.value)

        with pytest.raises(DataError) as err:
            in_memory_rowset.row_count
        assert "No results available" in str(err.value)

        with pytest.raises(DataError) as err:
            in_memory_rowset.statistics
        assert "No results available" in str(err.value)

        # But we can directly check the internal state
        assert len(in_memory_rowset._sync_row_set._row_sets) == 0
        assert in_memory_rowset._sync_row_set._current_row_set_idx == 0
        assert in_memory_rowset._sync_row_set._current_row == -1

    def test_append_empty_response(self, in_memory_rowset):
        """Test appending an empty response."""
        in_memory_rowset.append_empty_response()

        assert in_memory_rowset.row_count == -1
        assert in_memory_rowset.columns == []
        assert in_memory_rowset.statistics is None

    async def test_append_response(self, in_memory_rowset, mock_response):
        """Test appending a response with data."""
        # Create a proper aclose method
        async def mock_aclose():
            mock_response.is_closed = True

        mock_response.aclose = mock_aclose
        mock_response.is_closed = False

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
        assert mock_response.is_closed is True

    async def test_append_response_empty_content(
        self, in_memory_rowset, mock_empty_response
    ):
        """Test appending a response with empty content."""
        # Create a proper aclose method
        async def mock_aclose():
            mock_empty_response.is_closed = True

        mock_empty_response.aclose = mock_aclose
        mock_empty_response.is_closed = False

        await in_memory_rowset.append_response(mock_empty_response)

        assert in_memory_rowset.row_count == -1
        assert in_memory_rowset.columns == []

        # Verify response is closed
        assert mock_empty_response.is_closed is True

    async def test_append_response_invalid_json(
        self, in_memory_rowset, mock_invalid_json_response
    ):
        """Test appending a response with invalid JSON."""
        # Create a proper aclose method
        async def mock_aclose():
            mock_invalid_json_response.is_closed = True

        mock_invalid_json_response.aclose = mock_aclose
        mock_invalid_json_response.is_closed = False

        with pytest.raises(DataError) as err:
            await in_memory_rowset.append_response(mock_invalid_json_response)

        assert "Invalid query data format" in str(err.value)

        # Verify response is closed even if there's an error
        assert mock_invalid_json_response.is_closed is True

    async def test_append_response_missing_meta(
        self, in_memory_rowset, mock_missing_meta_response
    ):
        """Test appending a response with missing meta field."""
        # Create a proper aclose method
        async def mock_aclose():
            mock_missing_meta_response.is_closed = True

        mock_missing_meta_response.aclose = mock_aclose
        mock_missing_meta_response.is_closed = False

        with pytest.raises(DataError) as err:
            await in_memory_rowset.append_response(mock_missing_meta_response)

        assert "Invalid query data format" in str(err.value)

        # Verify response is closed even if there's an error
        assert mock_missing_meta_response.is_closed is True

    async def test_append_response_missing_data(
        self, in_memory_rowset, mock_missing_data_response
    ):
        """Test appending a response with missing data field."""
        # Create a proper aclose method
        async def mock_aclose():
            mock_missing_data_response.is_closed = True

        mock_missing_data_response.aclose = mock_aclose
        mock_missing_data_response.is_closed = False

        with pytest.raises(DataError) as err:
            await in_memory_rowset.append_response(mock_missing_data_response)

        assert "Invalid query data format" in str(err.value)

        # Verify response is closed even if there's an error
        assert mock_missing_data_response.is_closed is True

    async def test_nextset_no_more_sets(self, in_memory_rowset, mock_response):
        """Test nextset when there are no more result sets."""
        # Create a proper aclose method
        async def mock_aclose():
            pass

        mock_response.aclose = mock_aclose

        await in_memory_rowset.append_response(mock_response)
        assert await in_memory_rowset.nextset() is False

    async def test_nextset_with_more_sets(self, in_memory_rowset, mock_response):
        """Test nextset when there are more result sets.

        The implementation seems to add rowsets correctly, but behaves differently
        than expected when accessing them via nextset.
        """
        # Create a proper aclose method
        async def mock_aclose():
            pass

        mock_response.aclose = mock_aclose

        # Add two result sets directly
        await in_memory_rowset.append_response(mock_response)
        await in_memory_rowset.append_response(mock_response)

        # We should have 2 result sets now, but can only access the first one initially
        assert len(in_memory_rowset._sync_row_set._row_sets) == 2
        assert in_memory_rowset._sync_row_set._current_row_set_idx == 0

        # Move to the second result set
        assert await in_memory_rowset.nextset() is True
        assert in_memory_rowset._sync_row_set._current_row_set_idx == 1

        # Try to move beyond - should return False
        assert await in_memory_rowset.nextset() is False
        assert (
            in_memory_rowset._sync_row_set._current_row_set_idx == 1
        )  # Should stay at last set

    async def test_iteration(self, in_memory_rowset, mock_response):
        """Test row iteration."""
        # Create a proper aclose method
        async def mock_aclose():
            pass

        mock_response.aclose = mock_aclose

        await in_memory_rowset.append_response(mock_response)

        # Test __anext__ directly
        row1 = await in_memory_rowset.__anext__()
        assert row1 == [1, "one"]

        row2 = await in_memory_rowset.__anext__()
        assert row2 == [2, "two"]

        # Should raise StopAsyncIteration when done
        with pytest.raises(StopAsyncIteration):
            await in_memory_rowset.__anext__()

    async def test_iteration_after_nextset(self, in_memory_rowset, mock_response):
        """Test row iteration after nextset.

        This test is tricky because in the mock setup, the second row set
        is actually empty despite us adding the same mock response.
        """
        # Create a proper aclose method
        async def mock_aclose():
            pass

        mock_response.aclose = mock_aclose

        # Add first result set (with data)
        await in_memory_rowset.append_response(mock_response)

        # Read rows from first set
        rows1 = []
        try:
            while True:
                rows1.append(await in_memory_rowset.__anext__())
        except StopAsyncIteration:
            # This is expected after exhausting the first result set
            pass

        assert len(rows1) == 2
        assert rows1 == [[1, "one"], [2, "two"]]

        # Create a new response with empty content for the second set
        empty_response = MagicMock(spec=Response)

        async def mock_empty_aiter_bytes():
            yield b""

        empty_response.aiter_bytes.return_value = mock_empty_aiter_bytes()
        empty_response.aclose = mock_aclose

        # Add an empty second result set
        await in_memory_rowset.append_response(empty_response)

        # Verify we have 2 result sets
        assert len(in_memory_rowset._sync_row_set._row_sets) == 2

        # Move to the second set
        assert await in_memory_rowset.nextset() is True

        # Verify we're positioned correctly
        assert in_memory_rowset._sync_row_set._current_row_set_idx == 1

        # Verify the second set is empty
        assert in_memory_rowset._sync_row_set._row_sets[1].row_count == -1
        assert in_memory_rowset._sync_row_set._row_sets[1].rows == []

        # Attempting to read from an empty set should raise DataError
        with pytest.raises(DataError) as err:
            await in_memory_rowset.__anext__()
        assert "no rows to fetch" in str(err.value)

    async def test_empty_rowset_iteration(self, in_memory_rowset):
        """Test iteration of an empty rowset."""
        in_memory_rowset.append_empty_response()

        # Empty rowset should raise DataError, not StopAsyncIteration
        with pytest.raises(DataError) as err:
            await in_memory_rowset.__anext__()

        assert "no rows to fetch" in str(err.value)

    async def test_aclose(self, in_memory_rowset, mock_response):
        """Test aclose method."""
        # Create a proper aclose method
        async def mock_aclose():
            pass

        mock_response.aclose = mock_aclose

        # Set up a spy on the sync row_set's close method
        with patch.object(in_memory_rowset._sync_row_set, "close") as mock_close:
            await in_memory_rowset.append_response(mock_response)
            await in_memory_rowset.aclose()

            # Verify sync close was called
            mock_close.assert_called_once()
