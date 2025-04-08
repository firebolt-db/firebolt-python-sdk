from typing import Dict
from unittest.mock import MagicMock

from pytest import fixture, mark

from firebolt.common.cursor.base_cursor import BaseCursor
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
