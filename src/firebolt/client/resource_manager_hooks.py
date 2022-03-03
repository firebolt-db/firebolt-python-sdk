from json import JSONDecodeError
from logging import getLogger

from httpx import HTTPStatusError, Request, RequestError, Response

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

    If an error is message is found raise as an ApiError
    """

    try:
        response.raise_for_status()
    except RequestError as exc:
        logger.debug(f"An error occurred while requesting {exc.request.url!r}.")
        raise exc
    except HTTPStatusError as exc:
        response.read()  # without this, you can get a ResponseNotRead error
        try:
            parsed_response = exc.response.json()
        except JSONDecodeError:
            parsed_response = {"_raw": exc.response.text}
        debug_message = (
            f"Error response {exc.response.status_code} "
            f"while requesting {exc.request.url!r}. "
            f"Response: {parsed_response}. "
        )
        if "message" in parsed_response:
            error_message = parsed_response["message"]
            logger.debug(f"{debug_message} Message: {error_message}")
            raise RuntimeError(error_message) from exc
        logger.debug(debug_message)
        raise exc
