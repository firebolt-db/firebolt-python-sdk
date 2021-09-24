from contextlib import contextmanager
from typing import Generator, Optional

from firebolt.client.client import FireboltClient
from firebolt.client.hooks import log_request, log_response, raise_on_4xx_5xx
from firebolt.common.exception import (
    FireboltClientLookupError,
    FireboltClientRequiredError,
)
from firebolt.common.settings import Settings

_firebolt_clients: set[FireboltClient] = set()


def get_firebolt_client() -> FireboltClient:
    global _firebolt_clients
    if len(_firebolt_clients) == 0:
        raise FireboltClientRequiredError()
    if len(_firebolt_clients) > 1:
        raise FireboltClientLookupError(
            f"Found {len(_firebolt_clients)} active Firebolt Clients. "
            f"You need to specify which one explicitly!"
        )
    return next(iter(_firebolt_clients))


@contextmanager
def init_firebolt_client(
    settings: Optional[Settings] = None,
) -> Generator[FireboltClient, None, None]:
    """
    Initialize FireboltClient.

    All sdk methods should be called inside this context in order to have an access
    to client.
    """
    global _firebolt_clients
    settings = settings or Settings()
    client = FireboltClient(
        auth=(settings.user, settings.password.get_secret_value()),
        base_url=f"https://{settings.server}",
        api_endpoint=settings.server,
    )
    client.event_hooks = {
        "request": [log_request],
        "response": [log_response, raise_on_4xx_5xx],
    }
    _firebolt_clients.add(client)
    yield client
    client.close()
    _firebolt_clients.remove(client)
