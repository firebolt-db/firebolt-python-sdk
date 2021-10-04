from logging import getLogger

from httpx import HTTPStatusError, Request, RequestError, Response

from firebolt.common.exception import BadRequestError

logger = getLogger(__name__)


def log_request(request: Request) -> None:
    """Hook to log http requests"""
    logger.debug(
        f"Request event hook: {request.method} {request.url} - Waiting for response"
    )


def log_response(response: Response) -> None:
    """Hook to log responses to http requests"""
    request = response.request
    logger.debug(
        f"Response event hook: {request.method} {request.url}"
        f" - Status {response.status_code}"
    )


def raise_on_4xx_5xx(response: Response) -> None:
    """
    Hook to raise an error on http responses with codes indicating an error.

    If a 400 code is found, raise a follow-on BadRequestError, attempting to
    indicate more specifically how the request is bad.
    """
    try:
        response.raise_for_status()
    except RequestError as exc:
        logger.exception(f"An error occurred while requesting {exc.request.url!r}.")
        raise exc
    except HTTPStatusError as exc:
        response.read()  # without this, you can get a ResponseNotRead error
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
