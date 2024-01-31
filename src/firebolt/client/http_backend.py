import socket
from typing import Any

try:
    from httpcore.backends.auto import AutoBackend  # type: ignore
    from httpcore.backends.sync import SyncBackend  # type: ignore
except ImportError:
    from httpcore._backends.auto import AutoBackend  # type: ignore
    from httpcore._backends.sync import SyncBackend  # type: ignore

from httpx import AsyncHTTPTransport, HTTPTransport

from firebolt.common.constants import KEEPALIVE_FLAG, KEEPIDLE_RATE


def override_stream(stream):  # type: ignore [no-untyped-def]
    keepidle = getattr(socket, "TCP_KEEPIDLE", 0x10)  # 0x10 is TCP_KEEPALIVE on mac

    sock = (
        stream.get_extra_info("socket")
        if hasattr(stream, "get_extra_info")
        else stream.sock
    )

    # Enable keepalive
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, KEEPALIVE_FLAG)
    # Set keepalive to 60 seconds
    sock.setsockopt(socket.IPPROTO_TCP, keepidle, KEEPIDLE_RATE)
    return stream


class AsyncOverriddenHttpBackend(AutoBackend):
    """
    `OverriddenHttpBackend` is a short-term solution for the TCP
    connection idle timeout issue described in the following article:
    https://docs.aws.amazon.com/elasticloadbalancing/latest/network/network-load-balancers.html#connection-idle-timeout
    Since httpx creates a connection right before executing a request, the
    backend must be overridden to set the socket to `KEEPALIVE`
    and `KEEPIDLE` settings.
    """

    async def connect_tcp(self, *args, **kwargs):  # type: ignore
        stream = await super().connect_tcp(*args, **kwargs)
        return override_stream(stream)

    async def open_tcp_stream(self, *args, **kwargs):  # type: ignore
        stream = await super().open_tcp_stream(*args, **kwargs)
        return override_stream(stream)


class OverriddenHttpBackend(SyncBackend):
    """
    `OverriddenHttpBackend` is a short-term solution for the TCP
    connection idle timeout issue described in the following article:
    https://docs.aws.amazon.com/elasticloadbalancing/latest/network/network-load-balancers.html#connection-idle-timeout
    Since httpx creates a connection right before executing a request, the
    backend must be overridden to set the socket to `KEEPALIVE`
    and `KEEPIDLE` settings.
    """

    def connect_tcp(self, *args, **kwargs):  # type: ignore
        stream = super().connect_tcp(*args, **kwargs)
        return override_stream(stream)

    def open_tcp_stream(self, *args, **kwargs):  # type: ignore
        stream = super().open_tcp_stream(*args, **kwargs)
        return override_stream(stream)


class AsyncKeepaliveTransport(AsyncHTTPTransport):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        if hasattr(self._pool, "_network_backend"):
            self._pool._network_backend = AsyncOverriddenHttpBackend()  # type: ignore
        if hasattr(self._pool, "_backend"):
            self._pool._backend = AsyncOverriddenHttpBackend()  # type: ignore


class KeepaliveTransport(HTTPTransport):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        if hasattr(self._pool, "_network_backend"):
            self._pool._network_backend = OverriddenHttpBackend()  # type: ignore
        if hasattr(self._pool, "_backend"):
            self._pool._backend = OverriddenHttpBackend()  # type: ignore
