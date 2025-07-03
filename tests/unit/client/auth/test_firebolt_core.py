"""Unit tests for FireboltCore authentication."""


from firebolt.client.auth import FireboltCore


def test_firebolt_core_default():
    """Test FireboltCore with default parameters."""
    auth = FireboltCore()
    assert auth.url == "http://localhost:3473"

    # Test the authentication interface is consistent
    assert hasattr(auth, "url")
    assert hasattr(auth, "token")
    assert auth.token == ""  # Empty token for Firebolt Core
    assert callable(getattr(auth, "get_firebolt_version"))
    assert auth.get_firebolt_version().name == "CORE"


def test_firebolt_core_custom_url():
    """Test FireboltCore with custom URL."""
    auth = FireboltCore(url="http://custom-host:8080")
    assert auth.url == "http://custom-host:8080"


def test_ipv6_connection_object():
    """Test FireboltCore auth object with IPv6 URLs."""
    # Create auth objects with IPv6 URLs
    auth1 = FireboltCore(url="http://[::1]:3473")
    auth2 = FireboltCore(url="http://[2001:db8::1]:8080")

    # Check the URLs are correctly formatted
    assert "[::1]" in auth1.url
    assert "[2001:db8::1]" in auth2.url
    assert ":3473" in auth1.url
    assert ":8080" in auth2.url
