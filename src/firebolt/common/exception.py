from inspect import cleandoc

from httpx import HTTPStatusError


class FireboltError(Exception):
    pass


class FireboltClientRequiredError(FireboltError):
    def __init__(
        self,
        message: str = cleandoc(
            """
            Firebolt Client not found. Start one in a context manager:
            ```
            with FireboltClient.from_env() as fc:
                ...
            ```
            """
        ),
    ):
        super().__init__(message)


class FireboltEngineError(FireboltError):
    """Base error for engine errors"""


class AlreadyBoundError(FireboltEngineError):
    pass


class EndpointRequiredError(FireboltEngineError):
    pass


class DatabaseRequiredError(FireboltEngineError):
    pass


class CursorError(FireboltError):
    pass


class CursorClosedError(CursorError):
    def __init__(self, method_name: str):
        self.method_name = method_name
        super.__repr__

    def __str__(self):
        return f"unable to call {self.method_name}: cursor closed"


class QueryNotRunError(CursorError):
    def __init__(self, method_name: str):
        self.method_name = method_name
        super.__repr__

    def __str__(self):
        return f"unable to call {self.method_name}: need to run a query first"


class QueryError(CursorError):
    pass


class BadRequestError(HTTPStatusError):
    pass
