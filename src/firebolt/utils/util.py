import logging
from functools import lru_cache
from os import environ
from time import time
from types import TracebackType
from typing import TYPE_CHECKING, Callable, Optional, Type, TypeVar

from httpx import URL, Response, codes

from firebolt.utils.exception import ConfigurationError

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


def _print_error_body(resp: Response) -> None:
    """log error body if it exists, since it's not always logged by default"""
    try:
        if (
            codes.is_error(resp.status_code)
            and "Content-Length" in resp.headers
            and int(resp.headers["Content-Length"]) > 0
        ):
            logger.error(f"Something went wrong: {resp.read().decode('utf-8')}")
    except Exception:
        pass


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
