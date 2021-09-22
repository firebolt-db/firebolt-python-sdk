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
    """Base error for engine errors."""


class AlreadyBoundError(FireboltEngineError):
    pass


class EndpointRequiredError(FireboltEngineError):
    pass


class DatabaseRequiredError(FireboltEngineError):
    pass


class BadRequestError(HTTPStatusError):
    pass


class AuthenticationError(FireboltError):
    """
    Firebolt authentication error. Stores error cause and authentication endpoint.
    """

    def __init__(self, cause: str, api_endpoint: str):
        self.cause = cause
        self.api_endpoint = api_endpoint

    def __str__(self) -> str:
        return f"Failed to authenticate at {self.api_endpoint}: {self.cause}"
