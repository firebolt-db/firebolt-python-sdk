import logging

import httpx
from pydantic import SecretStr

logger = logging.getLogger(__name__)


class BadRequestError(httpx.HTTPStatusError):
    pass


def get_access_token(host: str, username: str, password: SecretStr) -> str:
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
            json={"username": username, "password": password.get_secret_value()},
            timeout=None,
        )
        return response.json()["access_token"]


def get_http_client(host: str, access_token: str) -> httpx.Client:
    """
    Get an httpx client configured to talk to the Firebolt API.

    Args:
        host: Firebolt server (eg. api.app.firebolt.io)
        access_token: Access token for API Authorization.

    Returns:
         A httpx.Client configured to communicate with Firebolt.
    """

    # see: https://www.python-httpx.org/advanced/#event-hooks
    def log_request(request: httpx.Request) -> None:
        """Hook to log http requests"""
        logger.debug(
            f"Request event hook: {request.method} {request.url} - Waiting for response"
        )

    def log_response(response: httpx.Response) -> None:
        """Hook to log responses to http requests"""
        request = response.request
        logger.debug(
            f"Response event hook: {request.method} {request.url} - "
            f"Status {response.status_code}"
        )

    def raise_on_4xx_5xx(response: httpx.Response) -> None:
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
                f"Error response {exc.response.status_code} "
                f"while requesting {exc.request.url!r}. "
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
