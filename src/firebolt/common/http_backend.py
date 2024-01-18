import socket
from typing import Any, Optional

try:
    from httpcore.backends.auto import AutoBackend  # type: ignore [import]
    from httpcore.backends.base import (  # type: ignore [import]
        AsyncNetworkStream,
    )
except ImportError:
    from httpcore._backends.auto import AutoBackend  # type: ignore [import]
    from httpcore._backends.base import AsyncNetworkStream  # type: ignore [import]

from httpx import AsyncHTTPTransport

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
        **kwargs: Any,
    ) -> AsyncNetworkStream:
        stream = await super().connect_tcp(  # type: ignore [call-arg]
            host,
            port,
            timeout=timeout,
            local_address=local_address,
            **kwargs,
        )
        # Enable keepalive
        stream.get_extra_info("socket").setsockopt(
            socket.SOL_SOCKET, socket.SO_KEEPALIVE, KEEPALIVE_FLAG
        )
        # MacOS does not have TCP_KEEPIDLE
        if hasattr(socket, "TCP_KEEPIDLE"):
            keepidle = socket.TCP_KEEPIDLE
        else:
            keepidle = 0x10  # TCP_KEEPALIVE on mac

        # Set keepalive to 60 seconds
        stream.get_extra_info("socket").setsockopt(
            socket.IPPROTO_TCP, keepidle, KEEPIDLE_RATE
        )
        return stream


class AsyncKeepaliveTransport(AsyncHTTPTransport):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._pool._network_backend = AsyncOverriddenHttpBackend()
