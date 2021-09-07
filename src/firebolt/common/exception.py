from inspect import cleandoc


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


class FireboltEngineAlreadyBound(FireboltError):
    pass
