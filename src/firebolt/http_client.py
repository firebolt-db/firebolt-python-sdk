import logging

import httpx

logger = logging.getLogger(__name__)


class BadRequestError(httpx.HTTPStatusError):
    pass


def _get_token(host: str, username: str, password: str) -> str:
    """
    Authenticate with username and password, and get a Bearer token.

    Args:
        host: Firebolt server (eg. api.app.firebolt.io)
        username: Username, usually an email address
        password: Password

    Returns:
        Bearer Token
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

    Args:
        host: Firebolt server (eg. api.app.firebolt.io)
        username: Username, usually an email address
        password: Password

    Returns:
         A httpx.Client configured to communicate with Firebolt.
    """
    access_token = _get_token(host=host, username=username, password=password)

    # see: https://www.python-httpx.org/advanced/#event-hooks
    def log_request(request: httpx.Request):
        """Hook to log http requests"""
        logger.info(
            f"Request event hook: {request.method} {request.url} - Waiting for response"
        )

    def log_response(response: httpx.Response):
        """Hook to log responses to http requests"""
        request = response.request
        logger.info(
            f"Response event hook: {request.method} {request.url} - Status {response.status_code}"
        )

    def raise_on_4xx_5xx(response: httpx.Response):
        """
        Hook to raise an error on http responses with codes indicating an error.

        If a 400 code is found, raise a follow-on BadRequestError, attempting to
        indicate more specifically how the request is bad.
        """
        try:
            response.raise_for_status()
        except httpx.RequestError as exc:
            logger.exception(f"An error occurred while requesting {exc.request.url!r}.")
            raise exc
        except httpx.HTTPStatusError as exc:
            logger.exception(
                f"Error response {exc.response.status_code} while requesting {exc.request.url!r}. "
                f"Response: {exc.response.json()}"
            )
            if exc.response.status_code == 400:
                raise BadRequestError(
                    message=exc.response.json()["message"],
                    request=exc.request,
                    response=exc.response,
                ) from exc
            raise exc

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
