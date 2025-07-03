"""Unit tests for Firebolt Core utility functions."""

from pytest import raises

from firebolt.utils.exception import ConfigurationError
from firebolt.utils.firebolt_core import parse_firebolt_core_url


def test_ipv6_url_parsing():
    """Test IPv6 URL parsing for FireboltCore."""
    # Test with different IPv6 address formats
    ipv6_urls = [
        ("http://[::1]:3473", "http", "::1", 3473),
        ("http://[2001:db8::1]:8080", "http", "2001:db8::1", 8080),
        (
            "https://[2001:db8:85a3:8d3:1319:8a2e:370:7348]:443",
            "https",
            "2001:db8:85a3:8d3:1319:8a2e:370:7348",
            443,
        ),
    ]

    for url, expected_protocol, expected_host, expected_port in ipv6_urls:
        parsed = parse_firebolt_core_url(url)

        assert parsed.scheme == expected_protocol
        assert parsed.hostname == expected_host
        assert parsed.port == expected_port


def test_url_parsing():
    """Test URL parsing for FireboltCore."""
    # Test with different URL formats
    urls = [
        # Default values when no URL is provided
        (None, "http", "localhost", 3473),
        # Full URL
        ("http://example.com:8080", "http", "example.com", 8080),
        # Protocol only
        ("https://localhost:3473", "https", "localhost", 3473),
        # Host only
        ("http://custom-host:3473", "http", "custom-host", 3473),
        # Port only
        ("http://localhost:9999", "http", "localhost", 9999),
    ]

    for url, expected_protocol, expected_host, expected_port in urls:
        parsed = parse_firebolt_core_url(url)

        assert parsed.scheme == expected_protocol
        assert parsed.hostname == expected_host
        assert parsed.port == expected_port


def test_invalid_url_parsing():
    """Test that invalid URLs with forbidden protocols raise exceptions."""
    # Based on the implementation, only invalid protocols are actually checked
    invalid_urls = [
        "ftp://localhost:3473",  # Invalid protocol
        "ssh://example.com:22",  # Invalid protocol
    ]

    for url in invalid_urls:
        try:
            parse_firebolt_core_url(url)
            assert False, f"Expected ConfigurationError for URL: {url}"
        except ConfigurationError:
            pass  # Expected exception

    # Test invalid port (too large)
    with raises(ConfigurationError, match="Invalid URL format"):
        parse_firebolt_core_url("http://localhost:70000")
