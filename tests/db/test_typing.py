from typing import Dict

from pytest import raises

from firebolt.common.exception import DataError
from firebolt.db.typing import parse_type, parse_value


def test_parse_type(types_map: Dict[str, type]) -> None:
    for type_name, t in types_map.items():
        parsed = parse_type(type_name)
        assert (
            parsed == t
        ), f"Error parsing type {type_name}: expected {str(t)}, got {str(parsed)}"

    with raises(DataError) as exc_info:
        parse_type(1)

    assert (
        str(exc_info.value) == "Invalid typename 1: str expected"
    ), "Invalid type parsing error message"


def test_parse_value_int() -> None:
    assert parse_value(1, int) == 1, "Error parsing integer: provided int"
    assert parse_value("1", int) == 1, "Error parsing integer: provided str"
    assert parse_value(1.1, int) == 1, "Error parsing integer: provided float"
    assert parse_value(None, int) is None, "Error parsing integer: provided None"

    with raises(ValueError):
        parse_value("a", int)

    for val in ((1,), [1], Exception()):
        with raises(TypeError):
            parse_value(val, int)


def test_parse_value_float() -> None:
    assert parse_value(1, float) == 1.0, "Error parsing float: provided int"
    assert parse_value("1", float) == 1.0, "Error parsing float: provided str"
    assert parse_value("1.1", float) == 1.1, "Error parsing float: provided str"
    assert parse_value(1.1, float) == 1.1, "Error parsing float: provided float"
    assert parse_value(None, float) is None, "Error parsing float: provided None"

    with raises(ValueError):
        parse_value("a", float)

    for val in ((1.1,), [1.1], Exception()):
        with raises(TypeError):
            parse_value(val, float)


def test_parse_value_str() -> None:
    assert parse_value(1, str) == "1", "Error parsing str: provided int"
    assert parse_value("a", str) == "a", "Error parsing str: provided str"
    assert parse_value(1.1, str) == "1.1", "Error parsing str: provided float"
    assert parse_value(("a",), str) == "('a',)", "Error parsing str: provided tuple"
    assert parse_value(["a"], str) == "['a']", "Error parsing str: provided list"
    assert parse_value(None, str) is None, "Error parsing str: provided None"
