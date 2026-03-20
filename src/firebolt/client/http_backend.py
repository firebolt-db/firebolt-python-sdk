import anyio
import socket
import threading
import time
from typing import Any, Dict, List

try:
    from httpcore.backends.auto import AutoBackend  # type: ignore
    from httpcore.backends.sync import SyncBackend  # type: ignore
except ImportError:
    from httpcore._backends.auto import AutoBackend  # type: ignore
    from httpcore._backends.sync import SyncBackend  # type: ignore

from httpx import AsyncHTTPTransport, HTTPTransport, Request, Response

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


class DNSCache:
    def __init__(self, ttl: float = 30.0):
        self.ttl = ttl
        self.cache: Dict[str, List[str]] = {}
        self.expiry: Dict[str, float] = {}
        self.indices: Dict[str, int] = {}
        self._lock = threading.Lock()

    def get_ip_round_robin(self, hostname: str) -> str:
        now = time.monotonic()

        with self._lock:
            cached_ips = self.cache.get(hostname)
            expires_at = self.expiry.get(hostname, 0)

            if not cached_ips or now >= expires_at:
                try:
                    _, _, new_ips = socket.gethostbyname_ex(hostname)
                    if new_ips:
                        self.cache[hostname] = sorted(new_ips)
                        self.expiry[hostname] = now + self.ttl
                        cached_ips = self.cache[hostname]
                except Exception:
                    if not cached_ips:
                        raise

            # calculate round robin index
            current_index = self.indices.get(hostname, 0)
            target_ip = cached_ips[current_index % len(cached_ips)]

            self.indices[hostname] = (current_index + 1) % len(cached_ips)

            return target_ip


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
    _dns_cache = DNSCache(ttl=30.0)

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._client_side_lb = kwargs.pop("client_side_lb", False)
        super().__init__(*args, **kwargs)
        self._apply_custom_backend(self)
        self._transport_kwargs = kwargs
        self._ip_transports: Dict[str, AsyncHTTPTransport] = {}
        self._lock = anyio.Lock()

    def _apply_custom_backend(self, transport: AsyncHTTPTransport) -> None:
        pool = getattr(transport, "_pool", None)
        if pool:
            for attr in ["_network_backend", "_backend"]:
                if hasattr(pool, attr):
                    setattr(pool, attr, AsyncOverriddenHttpBackend())

    async def handle_async_request(self, request: Request) -> Response:
        if not self._client_side_lb:
            return await super().handle_async_request(request)

        hostname = request.url.host

        try:
            target_ip = self._dns_cache.get_ip_round_robin(hostname)
        except Exception:
            return await super().handle_async_request(request)

        # Lazy-load the lock to ensure it's bound to the correct event loop
        if self._lock is None:
            self._lock = anyio.Lock()

        async with self._lock:
            if target_ip not in self._ip_transports:
                new_transport = AsyncHTTPTransport(**self._transport_kwargs)
                self._apply_custom_backend(new_transport)
                self._ip_transports[target_ip] = new_transport
            sub_transport = self._ip_transports[target_ip]

        original_url = request.url
        request.url = request.url.copy_with(host=target_ip)
        try:
            return await sub_transport.handle_async_request(request)
        finally:
            request.url = original_url

    async def aclose(self) -> None:
        """
        Close the primary transport and all sub-transports created for load balancing.
        """
        # Close the base transport first
        await super().aclose()

        # Close all child transports created for specific IPs
        if self._ip_transports:
            async with anyio.create_task_group() as tg:
                # Gather all transports in task group and close them
                for transport in self._ip_transports.values():
                    tg.start_soon(transport.aclose)

                self._ip_transports.clear()


class KeepaliveTransport(HTTPTransport):
    _dns_cache = DNSCache(ttl=30.0)

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._client_side_lb = kwargs.pop("client_side_lb", False)
        super().__init__(*args, **kwargs)
        self._apply_custom_backend(self)
        self._transport_kwargs = kwargs
        self._ip_transports: Dict[str, HTTPTransport] = {}
        self._lock = threading.Lock()

    def _apply_custom_backend(self, transport: HTTPTransport) -> None:
        pool = getattr(transport, "_pool", None)
        if pool:
            for attr in ["_network_backend", "_backend"]:
                if hasattr(pool, attr):
                    setattr(pool, attr, OverriddenHttpBackend())

    def handle_request(self, request: Request) -> Response:
        if not self._client_side_lb:
            return super().handle_request(request)

        hostname = request.url.host

        try:
            target_ip = self._dns_cache.get_ip_round_robin(hostname)
        except Exception:
            return super().handle_request(request)

        with self._lock:
            if target_ip not in self._ip_transports:
                new_transport = HTTPTransport(**self._transport_kwargs)
                self._apply_custom_backend(new_transport)
                self._ip_transports[target_ip] = new_transport
            sub_transport = self._ip_transports[target_ip]

        original_url = request.url
        request.url = request.url.copy_with(host=target_ip)
        try:
            return sub_transport.handle_request(request)
        finally:
            request.url = original_url

    def close(self) -> None:
        """
        Close the primary transport and all sub-transports.
        """
        # Close the base transport first
        super().close()

        # Close all child transports created for specific IPs
        with self._lock:
            for transport in self._ip_transports.values():
                try:
                    transport.close()
                except Exception:
                    # Best effort to close others if one fails
                    pass
            self._ip_transports.clear()
