from datetime import date, datetime
from typing import Dict

from pytest import fixture

from firebolt.async_db import ARRAY, DECIMAL, Connection, Cursor, connect
from firebolt.client.auth import Auth
from tests.unit.db_conftest import *  # noqa


@fixture
async def connection(
    server: str,
    db_name: str,
    auth: Auth,
    engine_name: str,
    account_name: str,
    mock_connection_flow: Callable,
) -> Connection:
    mock_connection_flow()
    async with (
        await connect(
            engine_name=engine_name,
            database=db_name,
            auth=auth,
            account_name=account_name,
            api_endpoint=server,
        )
    ) as connection:
        yield connection


@fixture
async def cursor(connection: Connection) -> Cursor:
    return connection.cursor()


@fixture
def types_map() -> Dict[str, type]:
    base_types = {
        "int": int,
        "long": int,
        "float": float,
        "double": float,
        "text": str,
        "date": date,
        "date_ext": date,
        "pgdate": date,
        "timestamp": datetime,
        "timestamp_ext": datetime,
        "timestampntz": datetime,
        "timestamptz": datetime,
        "Nothing null": str,
        "Decimal(123, 4)": DECIMAL(123, 4),
        "Decimal(38,0)": DECIMAL(38, 0),
        # Invalid decimal format
        "Decimal(38)": str,
        "boolean": bool,
        "SomeRandomNotExistingType": str,
        "bytea": bytes,
    }
    array_types = {f"array({k})": ARRAY(v) for k, v in base_types.items()}
    nullable_arrays = {f"{k} null": v for k, v in array_types.items()}
    nested_arrays = {f"array({k})": ARRAY(v) for k, v in array_types.items()}
    return {**base_types, **array_types, **nullable_arrays, **nested_arrays}
