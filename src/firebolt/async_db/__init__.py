from firebolt.async_db.connection import Connection, connect
from firebolt.async_db.cursor import Cursor
from firebolt.common._types import (
    ARRAY,
    BINARY,
    DATETIME,
    DECIMAL,
    NUMBER,
    ROWID,
    STRING,
    STRUCT,
    Binary,
    Date,
    DateFromTicks,
    ExtendedType,
    Time,
    TimeFromTicks,
    Timestamp,
    TimestampFromTicks,
)
from firebolt.common.constants import ParameterStyle
from firebolt.utils.exception import (
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Warning,
)

apilevel = "2.0"
# threads may only share the module and connections, cursors should not be shared
threadsafety = 2
paramstyle = ParameterStyle.QMARK.value
"""
The parameter style for SQL queries. Supported values:
- 'qmark': Use ? as parameter placeholders (default, client-side substitution)
- 'native': Alias for 'qmark'
- 'fb_numeric': Use $1, $2, ... as placeholders (server-side, sent as query_parameters)
"""
