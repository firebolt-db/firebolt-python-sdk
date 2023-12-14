import logging
from unittest.mock import MagicMock

from pytest import fixture, mark

from firebolt.common.base_cursor import BaseCursor


@fixture
def cursor():
    cursor = BaseCursor()
    cursor.connection = MagicMock()
    return cursor


@mark.parametrize(
    "headers, expected_parameters",
    [
        (
            {"Firebolt-Update-Parameters": "database=value1, key2=value2"},
            {"database": "value1"},
        ),
        (
            {"Firebolt-Update-Parameters": "database =    value1  ,key3=  value3 "},
            {"database": "value1"},
        ),
    ],
)
def test_parse_response_headers(headers, expected_parameters, cursor, caplog):
    # Capture the debug messages
    with caplog.at_level(logging.DEBUG, logger="firebolt.common.base_cursor"):
        # Call the function with the mock headers
        cursor._parse_response_headers(headers)

    # Assert that the parameters have been correctly updated
    assert cursor.parameters == expected_parameters

    # Check that the debug message has been logged
    assert "Unknown parameter" in caplog.text
