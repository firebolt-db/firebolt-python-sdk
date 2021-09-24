from contextlib import contextmanager
from typing import Generator, Optional

from firebolt.client.client import (
    DEFAULT_API_URL,
    FireboltAuth,
    FireboltClient,
)
from firebolt.client.hooks import log_request, log_response, raise_on_4xx_5xx
from firebolt.client.singleton import get_firebolt_client  # TODO remove
from firebolt.common.exception import AuthenticationError
from firebolt.common.settings import Settings


@contextmanager
def init_firebolt_client(
    settings: Optional[Settings] = None,
) -> Generator[FireboltClient, None, None]:
    """
    Initialize FireboltClient.

    All sdk methods should be called inside this context in order to have an access
    to client.
    """
    settings = settings or Settings()
    _client = FireboltClient(
        auth=(settings.user, settings.password.get_secret_value()),
        base_url=f"https://{settings.server}",
        api_endpoint=settings.server,
    )
    _client.event_hooks = {
        "request": [log_request],
        "response": [log_response, raise_on_4xx_5xx],
    }
    yield _client
    _client.close()
