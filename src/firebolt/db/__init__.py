from firebolt.common._types import (
    ARRAY,
    BINARY,
    DATETIME,
    DECIMAL,
    NUMBER,
    ROWID,
    STRING,
    Binary,
    Date,
    DateFromTicks,
    Time,
    TimeFromTicks,
    Timestamp,
    TimestampFromTicks,
)
from firebolt.db.connection import Connection, connect
from firebolt.db.cursor import Cursor
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
paramstyle = "qmark"
