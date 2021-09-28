from typing import Optional

from firebolt.client.client import (
    DEFAULT_API_URL,
    FireboltAuth,
    FireboltClient,
)
from firebolt.client.hooks import log_request, log_response, raise_on_4xx_5xx
from firebolt.common.settings import Settings


def init_firebolt_client(settings: Optional[Settings] = None) -> FireboltClient:
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
    return _client
