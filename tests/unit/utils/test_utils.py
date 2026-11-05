import pytest
from httpx import Response, codes

from firebolt.utils.exception import FireboltError, FireboltStructuredError
from firebolt.utils.util import (
    get_internal_error_code,
    parse_url_and_params,
    raise_error_from_response,
)


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


@pytest.mark.parametrize(
    "url,expected_url,expected_params",
    [
        (
            "http://example.com/path?param1=value1&param2=value2",
            "http://example.com/path",
            {"param1": "value1", "param2": "value2"},
        ),
        (
            "example.com/path?param1=value1&param2=value2",
            "https://example.com/path",
            {"param1": "value1", "param2": "value2"},
        ),
        ("http://example.com/path", "http://example.com/path", {}),
        ("http://example.com/path?param1=", "http://example.com/path", {}),
        ("example.com/path?param1=", "https://example.com/path", {}),
    ],
)
def test_parse_url_and_params(url, expected_url, expected_params):
    parsed_url, parsed_params = parse_url_and_params(url)
    assert parsed_url == expected_url
    assert parsed_params == expected_params


@pytest.mark.parametrize(
    "url",
    [
        ("http://example.com/path?param1=value1&param1=value2"),
    ],
)
def test_parse_url_and_params_multiple_values(url):
    with pytest.raises(ValueError) as excinfo:
        parse_url_and_params(url)
    assert "Multiple values found for key 'param1'" in str(excinfo.value)


STRUCTURED_ERROR_BODY = (
    '{"errors": [{"description": "Line 1, Column 1: syntax error"}],'
    ' "query": {"query_id": "id"}}'
)


@pytest.mark.parametrize("content_type", ["text/plain", "application/json"])
def test_raise_error_from_response_structured(content_type):
    resp = Response(
        codes.BAD_REQUEST,
        text=STRUCTURED_ERROR_BODY,
        headers={"Content-Type": content_type},
    )
    with pytest.raises(FireboltStructuredError) as exc_info:
        raise_error_from_response(resp)
    assert "syntax error" in str(exc_info.value)


@pytest.mark.parametrize(
    "text",
    [
        "Plaintext error message",
        '{"message": "no errors key"}',
        "[1]",
        "",
        '{"errors": "boom"}',
        '{"errors": ["boom"]}',
        '{"errors": []}',
    ],
)
def test_raise_error_from_response_plaintext(text):
    resp = Response(
        codes.BAD_REQUEST, text=text, headers={"Content-Type": "text/plain"}
    )
    with pytest.raises(FireboltError) as exc_info:
        raise_error_from_response(resp)
    assert not isinstance(exc_info.value, FireboltStructuredError)
    assert str(exc_info.value) == text
