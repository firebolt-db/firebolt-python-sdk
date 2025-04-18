from typing import Any, Dict, Optional


class FireboltError(Exception):
    """Base class for all Firebolt errors."""


class FireboltEngineError(FireboltError):
    """Base class for engine related errors."""


class EngineNotRunningError(FireboltEngineError):
    """Engine that's being queried is not running."""

    def __init__(self, engine_name: str):
        super().__init__(f"Engine {engine_name} is not running")


class EngineNotFoundError(FireboltEngineError):
    """Engine with provided name was not found."""

    def __init__(self, engine_name: str):
        super().__init__(f"Engine with name {engine_name} was not found")


class DatabaseNotFoundError(FireboltError):
    """Database with provided name was not found."""

    def __init__(self, database_name: str):
        super().__init__(f"Database with name {database_name} was not found")


class InstanceTypeNotFoundError(FireboltError):
    """Instance type with provided name was not found."""

    def __init__(self, instance_type_name: str):
        super().__init__(f"Instance type with name {instance_type_name} was not found")


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


class AccountNotFoundOrNoAccessError(FireboltError):
    """Account with provided name doesn't exist.

    Args:
        account_name (str): Name of account that wasn't found

    Attributes:
        account_name (str): Name of account that wasn't found
    """

    def __init__(self, account_name: str):
        super().__init__(
            f"Account '{account_name}' does not exist "
            "in this organization or is not authorized. "
            "Please verify the account name and make sure your "
            "service account has the correct RBAC permissions and "
            "is linked to a user."
        )
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
    """Firebolt authorisation error.

    Args:
        cause (str): Reason for authorization failure

    """

    _default_error_message = (
        "Failed to connect to Firebolt. Could not authenticate with the given "
        "credentials. Please verify the provided credentials are up to date and "
        "correct and that you have the correct user permissions"
    )

    def __init__(self, cause: Optional[str] = None):
        error_cause = cause if cause else self._default_error_message
        super().__init__(f"Authorization failed: {error_cause}.")


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


class V1NotSupportedError(NotSupportedError):
    """Operation not supported in Firebolt V1

    Exception raised when trying to use the functionality
    that is not supported in Firebolt V1.
    """

    msg = (
        "{} is not supported in this version of Firebolt. "
        "Please contact support to upgrade your account to a new version."
    )

    def __init__(self, operation: str) -> None:

        super().__init__(self.msg.format(operation))


class ConfigurationError(InterfaceError):
    """Invalid configuration error."""


class FireboltStructuredError(ProgrammingError):
    """Base class for structured errors received in JSON body."""

    # Output will look like this after formatting:
    # "{severity}: {name} ({code}) - {description}, see {helpLink}"
    message_template = "{severity}{name}{code}{description}{at}{helpLink}"

    def __init__(self, json: Dict[str, Any]):
        self.json = json
        self.errors = json.get("errors", [])

    def __str__(self) -> str:
        error_messages = []
        for error in self.errors:
            severity = f"{error['severity']}: " if error.get("severity") else ""
            name = f"{error['name']} " if error.get("name") else ""
            code = f"({error['code']}) " if error.get("code") else ""
            description = (
                f"- {error['description']}" if error.get("description") else ""
            )
            helpLink = (  # NOSONAR: python:S608 compliant with the message template
                f", see {error['helpLink']}" if error.get("helpLink") else ""
            )
            at = f" at {error['location']}" if error.get("location") else ""
            message = self.message_template.format(
                severity=severity,
                name=name,
                code=code,
                description=description,
                helpLink=helpLink,
                at=at,
            )
            error_messages.append(message)
        return ",\n".join(error_messages)


class QueryTimeoutError(FireboltError, TimeoutError):
    """Query execution timed out.

    Exception raised when the query execution exceeds the specified timeout.
    """

    def __init__(self, message="Query execution timed out."):  # type: ignore
        super().__init__(message)


class MethodNotAllowedInAsyncError(FireboltError):
    """Method not allowed.

    Exception raised when the method is not allowed.
    """

    def __init__(self, method_name: str):
        super().__init__(
            f"Method {method_name} not allowed for an async query."
            " Please get the token and use the async query API to get the status."
        )
        self.method_name = method_name
