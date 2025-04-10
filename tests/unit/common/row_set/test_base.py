from typing import List, Optional

import pytest

from firebolt.common._types import RawColType
from firebolt.common.row_set.base import BaseRowSet
from firebolt.common.row_set.types import Column, Statistics


class TestBaseRowSet(BaseRowSet):
    """Concrete implementation of BaseRowSet for testing."""

    def __init__(
        self,
        row_count: int = 0,
        statistics: Optional[Statistics] = None,
        columns: Optional[List[Column]] = None,
    ):
        self._row_count = row_count
        self._statistics = statistics
        self._columns = columns or []

    @property
    def row_count(self) -> int:
        return self._row_count

    @property
    def statistics(self) -> Optional[Statistics]:
        return self._statistics

    @property
    def columns(self) -> List[Column]:
        return self._columns

    def nextset(self) -> bool:
        return False

    def append_empty_response(self) -> None:
        pass


class TestBaseRowSetClass:
    """Tests for BaseRowSet class."""

    @pytest.fixture
    def base_row_set(self):
        """Create a TestBaseRowSet instance."""
        columns = [
            Column(name="int_col", type_code=int),
            Column(name="str_col", type_code=str),
            Column(name="float_col", type_code=float),
        ]
        return TestBaseRowSet(row_count=2, columns=columns)

    def test_parse_row(self, base_row_set):
        """Test _parse_row method."""
        # Test with correct number of columns
        raw_row: List[RawColType] = ["1", "text", "1.5"]
        parsed_row = base_row_set._parse_row(raw_row)

        assert len(parsed_row) == 3
        assert parsed_row[0] == 1
        assert parsed_row[1] == "text"
        assert parsed_row[2] == 1.5

        # Test with None values
        raw_row_with_none: List[RawColType] = [None, None, None]
        parsed_row = base_row_set._parse_row(raw_row_with_none)

        assert len(parsed_row) == 3
        assert parsed_row[0] is None
        assert parsed_row[1] is None
        assert parsed_row[2] is None

    def test_parse_row_assertion_error(self, base_row_set):
        """Test _parse_row method with row length mismatch."""
        # Test with incorrect number of columns
        raw_row: List[RawColType] = ["1", "text"]  # Missing the third column

        with pytest.raises(AssertionError):
            base_row_set._parse_row(raw_row)

    def test_abstract_methods(self):
        """Test that BaseRowSet is an abstract class."""
        # This test verifies that BaseRowSet is abstract
        # We don't need to instantiate it directly
        assert hasattr(BaseRowSet, "__abstractmethods__")
        assert len(BaseRowSet.__abstractmethods__) > 0
