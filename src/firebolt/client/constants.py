from json import JSONDecodeError
from typing import Tuple, Type

from httpx import CookieConflict, HTTPError, InvalidURL, StreamError

DEFAULT_API_URL: str = "api.app.firebolt.io"
PROTOCOL_VERSION_HEADER_NAME = "Firebolt-Protocol-Version"
PROTOCOL_VERSION: str = "2.3"
_REQUEST_ERRORS: Tuple[Type, ...] = (
    HTTPError,
    InvalidURL,
    CookieConflict,
    StreamError,
    JSONDecodeError,
    KeyError,
    ValueError,
)
