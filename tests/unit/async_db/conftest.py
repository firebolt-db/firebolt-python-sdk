from datetime import date, datetime
from typing import Dict

from pytest import fixture
from pytest_asyncio import fixture as asyncio_fixture

from firebolt.async_db import ARRAY, DECIMAL, Connection, Cursor, connect
from firebolt.client.auth import Auth
from firebolt.common.settings import Settings
from tests.unit.db_conftest import *  # noqa


@asyncio_fixture
async def connection(settings: Settings, auth: Auth, db_name: str) -> Connection:
    async with (
        await connect(
            engine_url=settings.server,
            database=db_name,
            auth=auth,
            account_name=settings.account_name,
            api_endpoint=settings.server,
        )
    ) as connection:
        yield connection


@asyncio_fixture
async def cursor(connection: Connection, settings: Settings) -> Cursor:
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
