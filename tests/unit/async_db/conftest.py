from datetime import date, datetime
from typing import Dict

from pytest import fixture

from firebolt.async_db import (
    ARRAY,
    DATETIME64,
    DECIMAL,
    Connection,
    Cursor,
    connect,
)
from firebolt.common.settings import Settings
from tests.unit.db_conftest import *  # noqa


@fixture
async def connection(settings: Settings, db_name: str) -> Connection:
    async with (
        await connect(
            engine_url=settings.server,
            database=db_name,
            username="u",
            password="p",
            account_name=settings.account_name,
            api_endpoint=settings.server,
        )
    ) as connection:
        yield connection


@fixture
async def cursor(connection: Connection, settings: Settings) -> Cursor:
    return connection.cursor()


@fixture
def types_map() -> Dict[str, type]:
    base_types = {
        "UInt8": int,
        "UInt16": int,
        "UInt32": int,
        "Int32": int,
        "UInt64": int,
        "Int64": int,
        "Float32": float,
        "Float64": float,
        "String": str,
        "Date": date,
        "Date32": date,
        "DateTime": datetime,
        "DateTime64(7)": DATETIME64(7),
        "Nullable(Nothing)": str,
        "Decimal(123, 4)": DECIMAL(123, 4),
        "Decimal(38,0)": DECIMAL(38, 0),
        # Invalid decimal format
        "Decimal(38)": str,
        "SomeRandomNotExistingType": str,
    }
    array_types = {f"Array({k})": ARRAY(v) for k, v in base_types.items()}
    nullable_arrays = {f"Nullable({k})": v for k, v in array_types.items()}
    nested_arrays = {f"Array({k})": ARRAY(v) for k, v in array_types.items()}
    return {**base_types, **array_types, **nullable_arrays, **nested_arrays}
