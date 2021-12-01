from json import JSONDecodeError
from typing import Tuple, Type

from httpx import CookieConflict, HTTPError, InvalidURL, StreamError

DEFAULT_API_URL: str = "api.app.firebolt.io"
_REQUEST_ERRORS: Tuple[Type, ...] = (
    HTTPError,
    InvalidURL,
    CookieConflict,
    StreamError,
    JSONDecodeError,
    KeyError,
    ValueError,
)
