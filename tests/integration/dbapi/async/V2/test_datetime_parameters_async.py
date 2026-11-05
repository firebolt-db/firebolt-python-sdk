from datetime import datetime, timedelta, timezone

from pytest import MonkeyPatch, mark

import firebolt.async_db
from firebolt.async_db import Connection
from tests.integration.dbapi.conftest import generate_unique_table_name


@mark.parametrize("session_timezone", ["UTC", "Europe/Berlin"])
@mark.parametrize("aware", [False, True])
@mark.parametrize("paramstyle", ["qmark", "fb_numeric"])
async def test_datetime_parameters(
    connection: Connection,
    monkeypatch: MonkeyPatch,
    session_timezone: str,
    aware: bool,
    paramstyle: str,
) -> None:
    monkeypatch.setattr(firebolt.async_db, "paramstyle", paramstyle)
    value = datetime(2024, 7, 1, 12, 0, 0, 123456)
    session_offset = (
        timezone.utc if session_timezone == "UTC" else timezone(timedelta(hours=2))
    )
    if aware:
        value = value.replace(tzinfo=timezone(timedelta(hours=5, minutes=45)))
        expected_ts = value.astimezone(session_offset).replace(tzinfo=None)
        expected_tz = value.astimezone(session_offset)
    else:
        expected_ts = value
        expected_tz = value.replace(tzinfo=session_offset)

    input_type = "TIMESTAMPTZ" if aware else "TIMESTAMP"
    # Server-side JSON parameters are TEXT and require explicit casts.
    placeholders = (
        ["?", "?"]
        if paramstyle == "qmark"
        else [f"CAST(${i} AS {input_type})" for i in (1, 2)]
    )
    table = generate_unique_table_name()
    connection.init_parameters["timezone"] = session_timezone
    async with connection.cursor() as c:
        await c.execute(f'CREATE TABLE "{table}" (ts TIMESTAMP, tz TIMESTAMPTZ)')
        try:
            await c.execute(
                f'INSERT INTO "{table}" VALUES ({", ".join(placeholders)})',
                [value, value],
            )
            await c.execute(f'SELECT ts, tz FROM "{table}"')
            assert await c.fetchone() == [expected_ts, expected_tz]

            await c.execute(
                f'SELECT COUNT(*) FROM "{table}" WHERE ts = {placeholders[0]} AND tz = {placeholders[1]}',
                [value, value],
            )
            assert await c.fetchone() == [1]

            await c.execute(f"SELECT {placeholders[0]}", [value])
            assert await c.fetchone() == [value]
        finally:
            await c.execute(f'DROP TABLE "{table}"')
