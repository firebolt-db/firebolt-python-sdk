import pytest
from httpx import Response

from firebolt.utils.util import get_internal_error_code


@pytest.mark.parametrize(
    "status_code, content, expected_error_code",
    [
        (200, b"No error code here", None),
        (500, b"No error code here", None),
        (
            500,
            b"HTTP status code: 401 Unauthorized, body: failed to verify JWT token",
            401,
        ),
        (500, b"HTTP status code: 401 Unauthorized", 401),
        (
            500,
            b"HTTP status code: Unauthorized, body: failed to verify JWT token",
            None,
        ),
    ],
)
def test_get_internal_error_code(status_code, content, expected_error_code):
    response = Response(status_code=status_code, content=content)
    assert get_internal_error_code(response) == expected_error_code
