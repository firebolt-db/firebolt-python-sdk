from json import JSONDecodeError
from typing import Optional, Tuple, Type

from httpx import CookieConflict, HTTPError, InvalidURL, StreamError

DEFAULT_API_URL: str = "api.app.firebolt.io"
API_REQUEST_TIMEOUT_SECONDS: Optional[int] = 60
_REQUEST_ERRORS: Tuple[Type, ...] = (
    HTTPError,
    InvalidURL,
    CookieConflict,
    StreamError,
    JSONDecodeError,
    KeyError,
    ValueError,
)
