class FireboltError(Exception):
    """Base class for all Firebolt errors."""


class FireboltEngineError(FireboltError):
    """Base class for engine related errors."""


class EngineNotRunningError(FireboltEngineError):
    """Engine that's being queried is not running."""


class NoAttachedDatabaseError(FireboltEngineError):
    """Engine that's being accessed is not running.

    Args:
        method_name (str): Method that caused the error

    Attributes:
        method_name (str): Method that caused the error
    """

    def __init__(self, method_name: str):
        super().__init__(
            f"Unable to call {method_name}: "
            "Engine must be attached to a database first."
        )
        self.method_name = method_name


class AlreadyBoundError(FireboltEngineError):
    """Engine is already bound to a database."""


class FireboltDatabaseError(FireboltError):
    """Base class for database related errors."""


class AccountNotFoundError(FireboltError):
    """Account with provided name doesn't exist.

    Args:
        account_name (str): Name of account that wasn't found

    Attributes:
        account_name (str): Name of account that wasn't found
    """

    def __init__(self, account_name: str):
        super().__init__(f"Account '{account_name}' does not exist.")
        self.account_name = account_name


class AttachedEngineInUseError(FireboltDatabaseError):
    """Engine unavailable because it's starting/stopping.

    Args:
        method_name (str): Method that caused the error

    Attributes:
        method_name (str): Method that caused the error
    """

    def __init__(self, method_name: str):
        super().__init__(
            f"Unable to call {method_name}: "
            "Engine must not be in starting or stopping state."
        )
        self.method_name = method_name


class ConnectionError(FireboltError):
    """Base class for connection related errors."""


class ConnectionClosedError(ConnectionError):
    """Connection operations are unavailable since it's closed."""


class CursorError(FireboltError):
    """Base class for cursor related errors."""


class CursorClosedError(CursorError):
    """Cursor operations are unavailable since it's closed.

    Args:
        method_name (str): Method that caused the error

    Attributes:
        method_name (str): Method that caused the error
    """

    def __init__(self, method_name: str):
        super().__init__(f"Unable to call {method_name}: cursor closed.")
        self.method_name = method_name


class QueryNotRunError(CursorError):
    """Some cursor methods are unavailable before a query is run.

    Args:
        method_name (str): Method that caused the error

    Attributes:
        method_name (str): Method that caused the error
    """

    def __init__(self, method_name: str):
        super().__init__(f"Unable to call {method_name}: need to run a query first.")
        self.method_name = method_name


class AuthenticationError(FireboltError):
    """Firebolt authentication error.

    Stores error cause and authentication endpoint.

    Args:
        api_endpoint (str): Environment api endpoint used for authentication
        cause (str): Reason for authentication failure

    Attributes:
        api_endpoint (str): Environment api endpoint used for authentication
        cause (str): Reason for authentication failure
    """

    def __init__(self, cause: str):
        super().__init__(f"Failed to authenticate: {cause}.")
        self.cause = cause


class AuthorizationError(FireboltError):
    """Firebolt authentication error.

    Args:
        cause (str): Reason for authorization failure

    Attributes:
        cause (str): Reason for authorization failure
    """

    def __init__(self, cause: str):
        super().__init__(f"Authorization failed: {cause}.")


# PEP-249


class Warning(Exception):
    """Base class for warning exceptions.

    Exception raised for important warnings, like data truncations while inserting, etc.
    """


Error = FireboltError


class InterfaceError(Error):
    """Database interface related error.

    Exception raised for errors that are related to the database interface
    rather than the database itself.
    """


class DatabaseError(Error):
    """Exception raised for errors that are related to the database."""


class DataError(DatabaseError):
    """Data processing error.

    Exception raised for errors that are due to problems with the processed data,
    like division by zero, numeric value out of range, etc.
    """


class OperationalError(DatabaseError):
    """Database operating error.

    Exception raised for errors that are related to the database's operation
    and not necessarily under the control of the programmer, e.g., an unexpected
    disconnect occurs, the data source name is not found, a transaction could not
    be processed, a memory allocation error occurred during processing, etc.
    """


class IntegrityError(DatabaseError):
    """Database data integrity error.

    Exception raised when the relational integrity of the database is affected,
    e.g., a foreign key check fails.
    """


class InternalError(DatabaseError):
    """Database internal error.

    Exception raised when the database encounters an internal error,
    e.g., the cursor is not valid anymore, the transaction is out of sync, etc.
    """


class ProgrammingError(DatabaseError):
    """Database programming error.

    Exception raised for programming errors, e.g., table not found or already exists,
    syntax error in the SQL statement, wrong number of parameters specified, etc.
    """


class NotSupportedError(DatabaseError):
    """Operation not supported.

    Exception raised in case a method or database API was used which is not supported
    by the database, e.g., requesting a .rollback() on a connection that
    does not support transaction or has transactions turned off.
    """


class ConfigurationError(InterfaceError):
    """Invalid configuration error."""


class AsyncExecutionUnavailableError(ProgrammingError):
    """
    If `use_standard_sql` is specified the query status endpoint returns a JSON
    object with empty values instead of a proper status object. In that case,
    it is not possible to retrieve the results of an asynchronous query.
    """

    def __init__(self, error_message):  # type: ignore
        super().__init__(error_message)
