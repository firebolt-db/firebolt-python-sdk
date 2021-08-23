import logging

import httpx

logger = logging.getLogger(__name__)


def _get_token(host: str, username: str, password: str) -> str:
    """
    Authenticate with username and password, and get a Bearer token.

    :param host: Firebolt server (eg. api.app.firebolt.io)
    :param username: Username, should be an entire email address
    :param password: Password
    :return: Bearer Token string
    """
    with httpx.Client(http2=True) as client:
        response = client.post(
            f"https://{host}/auth/v1/login",
            headers={"Content-Type": "application/json;charset=UTF-8"},
            json={"username": username, "password": password},
            timeout=None,
        )
        return response.json()["access_token"]


def get_http_client(host: str, username: str, password: str) -> httpx.Client:
    """
    Get an httpx client configured to talk to the Firebolt API.

    :param host: Firebolt server (eg. api.app.firebolt.io)
    :param username: Username, should be an entire email address
    :param password: Password
    :return: A configured httpx.Client
    """
    access_token = _get_token(host=host, username=username, password=password)

    # see: https://www.python-httpx.org/advanced/#event-hooks
    def log_request(request):
        logger.info(
            f"Request event hook: {request.method} {request.url} - Waiting for response"
        )

    def log_response(response):
        request = response.request
        logger.info(
            f"Response event hook: {request.method} {request.url} - Status {response.status_code}"
        )

    def raise_on_4xx_5xx(response):
        response.raise_for_status()

    client = httpx.Client(
        http2=True,
        base_url=f"https://{host}",
        event_hooks={
            "request": [log_request],
            "response": [log_response, raise_on_4xx_5xx],
        },
        timeout=None,
    )
    client.headers.update({"Authorization": f"Bearer {access_token}"})
    return client
