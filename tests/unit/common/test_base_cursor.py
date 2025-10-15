from typing import Dict
from unittest.mock import MagicMock

from pytest import fixture, mark

from firebolt.common.cursor.base_cursor import (
    BaseCursor,
    _parse_remove_parameters,
    _parse_update_parameters,
)
from firebolt.common.statement_formatter import create_statement_formatter


@fixture
def cursor():
    cursor = BaseCursor(formatter=create_statement_formatter(version=2))
    cursor.connection = MagicMock()
    return cursor


@fixture
def initial_parameters() -> Dict[str, str]:
    return {"key1": "value1", "key2": "value2"}


@mark.parametrize(
    "set_params, expected",
    [
        (
            {"key2": "new_value2", "key3": "value3"},
            {"key1": "value1", "key2": "new_value2", "key3": "value3"},
        ),
        ({}, {"key1": "value1", "key2": "value2"}),
    ],
)
def test_update_set_parameters(
    set_params: Dict[str, str],
    expected: Dict[str, str],
    initial_parameters: Dict[str, str],
    cursor: BaseCursor,
):
    cursor._set_parameters = initial_parameters
    cursor._update_set_parameters(set_params)
    # Assert that the parameters have been correctly updated
    assert cursor._set_parameters == expected


def test_flush_parameters(initial_parameters: Dict[str, str], cursor: BaseCursor):
    cursor._set_parameters = initial_parameters
    cursor.flush_parameters()
    assert cursor._set_parameters == {}


def test_update_server_parameters_known_params(
    initial_parameters: Dict[str, str], cursor: BaseCursor
):
    cursor.parameters = initial_parameters
    cursor._update_set_parameters({"database": "new_database"})

    # Merge the dictionaries using the update() method
    updated_parameters = initial_parameters.copy()
    updated_parameters.update({"database": "new_database"})
    assert cursor.parameters == updated_parameters


@mark.parametrize(
    "header, expected",
    [
        (
            "key1=value1,key2=value2,key3=value3",
            {"key1": "value1", "key2": "value2", "key3": "value3"},
        ),
        (
            "key1 = value1 , key2= value2, key3 =value3",
            {"key1": "value1", "key2": "value2", "key3": "value3"},
        ),
    ],
)
def test_parse_update_parameters(header: str, expected: Dict[str, str]):
    """Test parsing update parameters header."""
    result = _parse_update_parameters(header)
    assert result == expected


@mark.parametrize(
    "header, expected",
    [
        ("key1,key2,key3", ["key1", "key2", "key3"]),
        (" key1 , key2, key3 ", ["key1", "key2", "key3"]),
    ],
)
def test_parse_remove_parameters(header: str, expected: list):
    """Test parsing remove parameters header."""
    result = _parse_remove_parameters(header)
    assert result == expected


@mark.parametrize(
    "initial_set_params, initial_params, params_to_remove, expected_set_params, expected_params",
    [
        (
            {"key1": "value1", "key2": "value2", "key3": "value3"},
            {"param1": "value1", "param2": "value2"},
            ["key1", "key3", "param1"],
            {"key2": "value2"},
            {"param2": "value2"},
        ),
        (
            {"key1": "value1", "key2": "value2"},
            {"param1": "value1"},
            ["nonexistent", "also_nonexistent"],
            {"key1": "value1", "key2": "value2"},
            {"param1": "value1"},
        ),
    ],
)
def test_remove_set_parameters(
    cursor: BaseCursor,
    initial_set_params: Dict[str, str],
    initial_params: Dict[str, str],
    params_to_remove: list,
    expected_set_params: Dict[str, str],
    expected_params: Dict[str, str],
):
    """Test removing parameters from cursor."""
    # Set up initial parameters
    cursor._set_parameters = initial_set_params
    cursor.parameters = initial_params

    # Remove parameters
    cursor._remove_set_parameters(params_to_remove)

    # Assert parameters were removed correctly
    assert cursor._set_parameters == expected_set_params
    assert cursor.parameters == expected_params
