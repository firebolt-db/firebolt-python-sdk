class FireboltError(Exception):
    pass


class FireboltEngineError(FireboltError):
    """Base error for engine errors."""


class EngineNotRunningError(FireboltEngineError):
    pass


class NoAttachedDatabaseError(FireboltEngineError):
    def __init__(self, method_name: str):
        self.method_name = method_name

    def __str__(self) -> str:
        return (
            f"Unable to call {self.method_name}: "
            "Engine must to be attached to a database first."
        )


class AlreadyBoundError(FireboltEngineError):
    pass


class FireboltDatabaseError(FireboltError):
    pass


class AccountNotFoundError(FireboltError):
    def __init__(self, method_name: str):
        self.method_name = method_name

    def __str__(self) -> str:
        return f'Account "{self.method_name}" does not exist.'


class AttachedEngineInUseError(FireboltDatabaseError):
    def __init__(self, method_name: str):
        self.method_name = method_name

    def __str__(self) -> str:
        return (
            f"Unable to call {self.method_name}: "
            "Engine must not be in starting or stopping state."
        )


class ConnectionError(FireboltError):
    pass


class ConnectionClosedError(ConnectionError):
    pass


class CursorError(FireboltError):
    pass


class CursorClosedError(CursorError):
    def __init__(self, method_name: str):
        self.method_name = method_name

    def __str__(self) -> str:
        return f"Unable to call {self.method_name}: cursor closed."


class QueryNotRunError(CursorError):
    def __init__(self, method_name: str):
        self.method_name = method_name

    def __str__(self) -> str:
        return f"Unable to call {self.method_name}: need to run a query first."


class QueryError(CursorError):
    pass


class AuthenticationError(FireboltError):
    """
    Firebolt authentication error. Stores error cause and authentication endpoint.
    """

    def __init__(self, cause: str, api_endpoint: str):
        self.cause = cause
        self.api_endpoint = api_endpoint

    def __str__(self) -> str:
        return f"Failed to authenticate at {self.api_endpoint}: {self.cause}."


# PEP-249


class Warning(Exception):
    """
    Exception raised for important warnings
    like data truncations while inserting, etc.
    """


Error = FireboltError


class InterfaceError(Error):
    """
    Exception raised for errors that are related to the database interface
    rather than the database itself.
    """


class DatabaseError(Error):
    """Exception raised for errors that are related to the database."""


class DataError(DatabaseError):
    """
    Exception raised for errors that are due to problems with the processed data
    like division by zero, numeric value out of range, etc.
    """


class OperationalError(DatabaseError):
    """
    Exception raised for errors that are related to the database's operation
    and not necessarily under the control of the programmer, e.g. an unexpected
    disconnect occurs, the data source name is not found, a transaction could not
    be processed, a memory allocation error occurred during processing, etc.
    """


class IntegrityError(DatabaseError):
    """
    Exception raised when the relational integrity of the database is affected,
    e.g. a foreign key check fails.
    """


class InternalError(DatabaseError):
    """
    Exception raised when the database encounters an internal error,
    e.g. the cursor is not valid anymore, the transaction is out of sync, etc.
    """


class ProgrammingError(DatabaseError):
    """
    Exception raised when the database encounters an internal error,
    e.g. the cursor is not valid anymore, the transaction is out of sync, etc.
    """


class NotSupportedError(DatabaseError):
    """
    Exception raised when the database encounters an internal error,
    e.g. the cursor is not valid anymore, the transaction is out of sync, etc.
    """


class ConfigurationError(InterfaceError):
    """
    Exception raised when provided configuration is not correct
    """
