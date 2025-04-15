from dataclasses import fields

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
