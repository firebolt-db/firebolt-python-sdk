from json import JSONDecodeError
from logging import getLogger

from httpx import HTTPStatusError, Request, RequestError, Response

logger = getLogger(__name__)


def log_request(request: Request) -> None:
    """Log HTTP requests.

    Hook for an HTTP client

    Args:
        request (Request): Request to log
    """
    logger.debug(
        "Request event hook: %s %s - Waiting for response", request.method, request.url
    )


def log_response(response: Response) -> None:
    """Log HTTP response.

    Hook for an HTTP client

    Args:
        response (Response): Response to log
    """
    request = response.request
    logger.debug(
        "Response event hook: %s %s - Status %s",
        request.method,
        request.url,
        response.status_code,
    )


def raise_on_4xx_5xx(response: Response) -> None:
    """Raise an error on HTTP response with error return code.

    Hook for an HTTP client.
    If an error message is found, raise as an ApiError.

    Args:
        response (Response): Response to check for error code

    Raises:
        RequestError: Error during performing request
        RuntimeError: Error processing request on server
        HTTPStatusError: HTTP error
    """
    try:
        response.raise_for_status()
    except RequestError as exc:
        logger.debug("An error occurred while requesting %s.", exc.request.url)
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
            logger.debug("%s Message: %s", debug_message, error_message)
            raise RuntimeError(error_message) from exc
        logger.debug(debug_message)
        raise exc
