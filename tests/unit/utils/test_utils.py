import pytest
from httpx import Response, codes

from firebolt.utils.util import get_internal_error_code


@pytest.mark.parametrize(
    "status_code, content, expected_error_code",
    [
        (codes.OK, b"No error code here", None),
        (codes.INTERNAL_SERVER_ERROR, b"No error code here", None),
        (
            codes.INTERNAL_SERVER_ERROR,
            b"HTTP status code: 401 Unauthorized, body: failed to verify JWT token",
            codes.UNAUTHORIZED,
        ),
        (
            codes.INTERNAL_SERVER_ERROR,
            b"HTTP status code: 401 Unauthorized",
            codes.UNAUTHORIZED,
        ),
        (
            codes.INTERNAL_SERVER_ERROR,
            b"HTTP status code: Unauthorized, body: failed to verify JWT token",
            None,
        ),
    ],
)
def test_get_internal_error_code(status_code, content, expected_error_code):
    response = Response(status_code=status_code, content=content)
    assert get_internal_error_code(response) == expected_error_code
