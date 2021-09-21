from contextlib import contextmanager
from inspect import cleandoc
from typing import Generator, Optional

from firebolt.client.client import FireboltClient
from firebolt.client.hooks import log_request, log_response, raise_on_4xx_5xx
from firebolt.common.exception import FireboltClientRequiredError
from firebolt.common.settings import Settings

_firebolt_client_singleton: Optional[FireboltClient] = None


def get_firebolt_client() -> FireboltClient:
    cleandoc(
        """
        Get the initialized FireboltClient singleton.

        This function is intended to be used by classes and functions that will
        be accessed within an existing FireboltClient context.
        """
    )
    global _firebolt_client_singleton
    if _firebolt_client_singleton is None:
        raise FireboltClientRequiredError()
    return _firebolt_client_singleton


@contextmanager
def init_firebolt_client(
    settings: Optional[Settings] = None,
) -> Generator[None, FireboltClient, None]:
    cleandoc(
        """
        Initialize FireboltClient singletone.

        All sdk methods should be called inside this context in order to have an access
        to client.
        """
    )
    global _firebolt_client_singleton
    settings = settings or Settings()
    _firebolt_client_singleton = FireboltClient(
        auth=(settings.user, settings.password.get_secret_value()),
        base_url=f"https://{settings.server}",
        api_endpoint=settings.server,
    )
    _firebolt_client_singleton.event_hooks = {
        "request": [log_request],
        "response": [log_response, raise_on_4xx_5xx],
    }
    yield _firebolt_client_singleton
    _firebolt_client_singleton.close()
    _firebolt_client_singleton = None
