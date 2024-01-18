try:
    # older version of pytest-httpx have their own Response class
    from pytest_httpx import to_response

    def Response(*args, **kwargs):
        # Allow passing data as content
        kwargs["data"] = kwargs.get("data", kwargs.get("content"))
        if "content" in kwargs:
            del kwargs["content"]
        return to_response(*args, **kwargs)

except ImportError:
    from httpx import Response  # noqa: F401
