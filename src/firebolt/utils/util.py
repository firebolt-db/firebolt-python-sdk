import logging
from functools import lru_cache
from os import environ
from time import time
from types import TracebackType
from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
)
from urllib.parse import parse_qs, urljoin, urlparse

from httpx import URL, Response, codes

from firebolt.utils.exception import (
    ConfigurationError,
    FireboltStructuredError,
)

T = TypeVar("T")
logger = logging.getLogger(__name__)


def cached_property(func: Callable[..., T]) -> T:
    """cached_property implementation for 3.7 backward compatibility.

    Args:
        func (Callable): Property getter

    Returns:
        T: Property of type, returned by getter
    """
    return property(lru_cache()(func))  # type: ignore


def prune_dict(d: dict) -> dict:
    """Prune items from dictionaries where value is None.

    Args:
        d (dict): Dict to prune

    Returns:
        dict: Pruned dict
    """
    return {k: v for k, v in d.items() if v is not None}


TMix = TypeVar("TMix")


def mixin_for(baseclass: Type[TMix]) -> Type[TMix]:
    """Define mixin with baseclass typehint.

    Should be used as a mixin base class to fix typehints.

    Args:
        baseclass (Type[TMix]): Class which mixin will be made for

    Returns:
        Type[TMix]: Mixin type to inherit from

    Examples:
        ```
        class ReadonlyMixin(mixin_for(BaseClass))):
            ...
        ```

    """
    if TYPE_CHECKING:
        return baseclass
    return object


def fix_url_schema(url: str) -> str:
    """Add schema to URL if it's missing.

    Args:
        url (str): URL to check

    Returns:
        str: URL with schema present

    """
    return url if url.startswith("http") else f"https://{url}"


def get_auth_endpoint(api_endpoint: URL) -> URL:
    """Create auth endpoint from api endpoint.

    Args:
        api_endpoint (URL): provided API endpoint

    Returns:
        URL: authentication endpoint
    """
    return api_endpoint.copy_with(
        host=".".join(["id"] + api_endpoint.host.split(".")[1:])
    )


def get_internal_error_code(response: Response) -> Optional[int]:
    """Get internal error code from response.

    Args:
        response (Response): HTTP response

    Returns:
        Optional[int]: Internal error code
    """
    # Internal server error usually hides the real error code in the response body
    if response.status_code == codes.INTERNAL_SERVER_ERROR:
        try:
            # Example response:
            # Received error from remote server
            # /core/v1/accounts/<account_num>/engines:getIdByName?engine_name=
            # <engine_name>. HTTP status code: 401 Unauthorized, body: failed to
            # verify JWT token: failed to verify jwt: "exp" not satisfied\n'
            error = int(response.text.split("HTTP status code: ")[1].split(" ")[0])
            body = (
                response.text.split("body: ")[1] if "body: " in response.text else None
            )
            logger.debug(
                f"Detected an internal server error with code: {error}, body: {body}"
            )
            return error
        except (IndexError, ValueError):
            return None
    return None


def merge_urls(base: URL, merge: URL) -> URL:
    """Merge a base and merge urls.

    If merge is not a relative url, do nothing

    Args:
        base (URL): Base URL to merge to
        merge (URL): URL to merge

    Returns:
        URL: Resulting URL
    """
    if merge.is_relative_url:
        merge_raw_path = base.raw_path + merge.raw_path.lstrip(b"/")
        return base.copy_with(raw_path=merge_raw_path)
    return merge


def validate_engine_name_and_url_v1(
    engine_name: Optional[str], engine_url: Optional[str]
) -> None:
    if engine_name and engine_url:
        raise ConfigurationError(
            "Both engine_name and engine_url are provided. Provide only one to connect."
        )


def raise_error_from_response(resp: Response) -> None:
    """
    Raise a correct error from the response.
    Look for a structured error in the body and raise it.
    If the body doesn't contain a structured error,
    log the body and raise a status code error.

    Args:
        resp (Response): HTTP response
    """
    to_raise = None
    try:
        decoded = resp.json()
        if "errors" in decoded and len(decoded["errors"]) > 0:
            # Raise later to avoid catching it in the except block
            to_raise = FireboltStructuredError(decoded)

    except Exception:
        # If we can't parse the body, print out the error body
        if "Content-Length" in resp.headers and int(resp.headers["Content-Length"]) > 0:
            logger.error(f"Something went wrong: {resp.read().decode('utf-8')}")

    if to_raise:
        raise to_raise

    # Raise status error if no error info was found in the body
    resp.raise_for_status()


class Timer:
    def __init__(self, message: str = ""):
        self._message = message

    def __enter__(self) -> "Timer":
        self._start_time: float = time()
        return self

    def __exit__(
        self,
        exc_type: Type[BaseException],
        exc_val: BaseException,
        exc_tb: TracebackType,
    ) -> None:
        self.elapsed_time: str = "{:.2f}".format(round((time() - self._start_time), 2))
        if (
            environ.get("FIREBOLT_SDK_PERFORMANCE_DEBUG", "0") == "1"
            and self._message != ""
        ):
            log_message = self._message + self.elapsed_time + "s"
            logger.debug(log_message)


def parse_url_and_params(url: str) -> Tuple[str, Dict[str, str]]:
    """Extract URL and query parameters separately from a URL."""
    url = fix_url_schema(url)
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    # This strips query parameters from the URL by joining base URL and path
    # skipping the query parameters.
    result_url = urljoin(url, parsed_url.path)
    # parse_qs returns a dictionary with values as lists.
    # We want the last value in the list.
    query_params_dict = {}
    for key, values in query_params.items():
        # Multiple values for the same key are not expected
        if len(values) > 1:
            raise ValueError(f"Multiple values found for key '{key}'")
        query_params_dict[key] = values[0]
    return result_url, query_params_dict


class _ExceptionGroup(Exception):
    """A base class for grouping exceptions.

    This class is used to create an exception group that can contain multiple
    exceptions. It is a placeholder for Python 3.11's ExceptionGroup, which
    allows for grouping exceptions together.
    """

    def __init__(self, message: str, exceptions: List[BaseException]):
        super().__init__(message)
        self.exceptions = exceptions

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.exceptions})"


ExceptionGroup = getattr(__builtins__, "ExceptionGroup", _ExceptionGroup)
