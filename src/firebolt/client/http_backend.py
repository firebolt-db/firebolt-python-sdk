import socket
from typing import Any, Iterable, Optional

try:
    from httpcore.backends.auto import AutoBackend  # type: ignore [import]
    from httpcore.backends.base import (  # type: ignore [import]
        SOCKET_OPTION,
        AsyncNetworkStream,
        NetworkStream,
    )
    from httpcore.backends.sync import SyncBackend  # type: ignore [import]
except ImportError:
    from httpcore._backends.auto import AutoBackend  # type: ignore [import]
    from httpcore._backends.base import (  # type: ignore [import]
        AsyncNetworkStream,
        NetworkStream,
        SOCKET_OPTION,
    )
    from httpcore._backends.sync import SyncBackend  # type: ignore [import]

from httpx import AsyncHTTPTransport, HTTPTransport

from firebolt.common.settings import KEEPALIVE_FLAG, KEEPIDLE_RATE


class AsyncOverriddenHttpBackend(AutoBackend):
    """
    `OverriddenHttpBackend` is a short-term solution for the TCP
    connection idle timeout issue described in the following article:
    https://docs.aws.amazon.com/elasticloadbalancing/latest/network/network-load-balancers.html#connection-idle-timeout
    Since httpx creates a connection right before executing a request, the
    backend must be overridden to set the socket to `KEEPALIVE`
    and `KEEPIDLE` settings.
    """

    async def connect_tcp(  # type: ignore [override]
        self,
        host: str,
        port: int,
        timeout: Optional[float] = None,
        local_address: Optional[str] = None,
        socket_options: Optional[Iterable[SOCKET_OPTION]] = None,
    ) -> AsyncNetworkStream:
        keepidle = getattr(socket, "TCP_KEEPIDLE", 0x10)  # 0x10 is TCP_KEEPALIVE on mac
        return await super().connect_tcp(  # type: ignore [call-arg]
            host,
            port,
            timeout=timeout,
            local_address=local_address,
            socket_options=[
                (socket.SOL_SOCKET, socket.SO_KEEPALIVE, KEEPALIVE_FLAG),
                (socket.IPPROTO_TCP, keepidle, KEEPIDLE_RATE),
                *(socket_options or []),
            ],
        )


class OverriddenHttpBackend(SyncBackend):
    """
    `OverriddenHttpBackend` is a short-term solution for the TCP
    connection idle timeout issue described in the following article:
    https://docs.aws.amazon.com/elasticloadbalancing/latest/network/network-load-balancers.html#connection-idle-timeout
    Since httpx creates a connection right before executing a request, the
    backend must be overridden to set the socket to `KEEPALIVE`
    and `KEEPIDLE` settings.
    """

    def connect_tcp(  # type: ignore [override]
        self,
        host: str,
        port: int,
        timeout: Optional[float] = None,
        local_address: Optional[str] = None,
        socket_options: Optional[Iterable[SOCKET_OPTION]] = None,
    ) -> NetworkStream:
        keepidle = getattr(socket, "TCP_KEEPIDLE", 0x10)  # 0x10 is TCP_KEEPALIVE on mac
        return super().connect_tcp(
            host,
            port,
            timeout=timeout,
            local_address=local_address,
            socket_options=[
                (socket.SOL_SOCKET, socket.SO_KEEPALIVE, KEEPALIVE_FLAG),
                (socket.IPPROTO_TCP, keepidle, KEEPIDLE_RATE),
                *(socket_options or []),
            ],
        )


class AsyncKeepaliveTransport(AsyncHTTPTransport):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._pool._network_backend = AsyncOverriddenHttpBackend()


class KeepaliveTransport(HTTPTransport):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._pool._network_backend = OverriddenHttpBackend()
