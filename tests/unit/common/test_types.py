from dataclasses import fields

from firebolt.common._types import ARRAY, DECIMAL, STRUCT
from firebolt.common.row_set.types import Column


def test_columns_supports_indexing():
    column = Column(
        name="test_column",
        type_code=int,
        display_size=10,
        internal_size=20,
        precision=5,
        scale=2,
        null_ok=True,
    )
    for i, field in enumerate(fields(column)):
        assert getattr(column, field.name) == column[i]


def test_array_is_hashable():
    """Test that ARRAY type is hashable and can be used in dictionaries and sets."""
    # Create ARRAY types
    array_of_int = ARRAY(int)
    array_of_str = ARRAY(str)
    array_of_array = ARRAY(ARRAY(int))

    # Test hash function works
    assert isinstance(hash(array_of_int), int)
    assert isinstance(hash(array_of_str), int)
    assert isinstance(hash(array_of_array), int)

    # Test equality with same hash values
    assert hash(array_of_int) == hash(ARRAY(int))
    assert hash(array_of_str) == hash(ARRAY(str))

    # Test usage in dictionary
    d = {array_of_int: "array_of_int", array_of_str: "array_of_str"}
    assert d[array_of_int] == "array_of_int"
    assert d[ARRAY(int)] == "array_of_int"

    # Test usage in set
    s = {array_of_int, array_of_str, array_of_array, ARRAY(int)}
    assert len(s) == 3  # array_of_int and ARRAY(int) are equal


def test_decimal_is_hashable():
    """Test that DECIMAL type is hashable and can be used in dictionaries and sets."""
    # Create DECIMAL types
    dec1 = DECIMAL(10, 2)
    dec2 = DECIMAL(5, 0)
    dec3 = DECIMAL(10, 2)  # Same as dec1

    # Test hash function works
    assert isinstance(hash(dec1), int)
    assert isinstance(hash(dec2), int)

    # Test equality with same hash values
    assert hash(dec1) == hash(dec3)
    assert dec1 == dec3

    # Test usage in dictionary
    d = {dec1: "dec1", dec2: "dec2"}
    assert d[dec1] == "dec1"
    assert d[DECIMAL(10, 2)] == "dec1"

    # Test usage in set
    s = {dec1, dec2, dec3}
    assert len(s) == 2  # dec1 and dec3 are the same


def test_struct_is_hashable():
    """Test that STRUCT type is hashable and can be used in dictionaries and sets."""
    # Create STRUCT types
    struct1 = STRUCT({"name": str, "age": int})
    struct2 = STRUCT({"value": DECIMAL(10, 2)})
    struct3 = STRUCT({"name": str, "age": int})  # Same as struct1
    nested_struct = STRUCT({"person": struct1, "balance": float})

    # Test hash function works
    assert isinstance(hash(struct1), int)
    assert isinstance(hash(struct2), int)
    assert isinstance(hash(nested_struct), int)

    # Test equality with same hash values
    assert hash(struct1) == hash(struct3)
    assert struct1 == struct3

    # Test usage in dictionary
    d = {struct1: "struct1", struct2: "struct2"}
    assert d[struct1] == "struct1"
    assert d[STRUCT({"name": str, "age": int})] == "struct1"

    # Test usage in set
    s = {struct1, struct2, struct3, nested_struct}
    assert len(s) == 3  # struct1 and struct3 are the same
