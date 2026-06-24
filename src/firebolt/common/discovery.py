from __future__ import annotations

from dataclasses import dataclass
from json import JSONDecodeError
from ssl import SSLContext
from typing import Any, Dict, Mapping, Optional, Union
from urllib.parse import urljoin, urlparse

from httpx import AsyncClient as HttpxAsyncClient
from httpx import Client as HttpxClient
from httpx import Timeout, codes

from firebolt.client.auth import Auth, FireboltCore
from firebolt.client.auth.base import FireboltAuthVersion
from firebolt.client.constants import DEFAULT_API_URL
from firebolt.common.base_connection import get_user_agent_for_connection
from firebolt.common.constants import DEFAULT_TIMEOUT_SECONDS
from firebolt.utils.exception import ConfigurationError, InterfaceError
from firebolt.utils.firebolt_core import get_core_certificate_context
from firebolt.utils.util import parse_url_and_params

DISCOVERY_PATH = "/.well-known/firebolt"
SSL_MODE_STRICT = "strict"
SSL_MODE_NONE = "none"
SSL_MODES = {SSL_MODE_STRICT, SSL_MODE_NONE}


@dataclass(frozen=True)
class DiscoveryConnectionInfo:
    engine_url: str
    api_endpoint: str
    parameters: Dict[str, Any]
    verify: Union[SSLContext, bool]


@dataclass(frozen=True)
class PreparedDiscoveryConnection:
    auth: Auth
    user_agent_header: str


def normalize_ssl_mode(ssl_mode: str) -> str:
    mode = ssl_mode.lower()
    if mode not in SSL_MODES:
        allowed = ", ".join(sorted(SSL_MODES))
        raise ConfigurationError(
            f"Invalid ssl_mode: {ssl_mode}. Expected one of: {allowed}."
        )
    return mode


def normalize_host(host: str, ssl_mode: str) -> str:
    """Normalize a discovery host into an HTTP(S) base URL."""
    if not host:
        raise ConfigurationError("host is required for discovery-based connections.")

    mode = normalize_ssl_mode(ssl_mode)
    default_scheme = "http" if mode == SSL_MODE_NONE else "https"
    raw_url = host if "://" in host else f"{default_scheme}://{host}"
    parsed = urlparse(raw_url)

    if parsed.scheme not in {"http", "https"}:
        raise ConfigurationError(
            f"Invalid host scheme: {parsed.scheme}. Expected 'http' or 'https'."
        )
    if not parsed.netloc:
        raise ConfigurationError(
            f"Invalid host: {host}. Expected a hostname, optionally with scheme and port."
        )
    if parsed.query or parsed.fragment:
        raise ConfigurationError(
            "host must not include query parameters or a fragment. "
            "Pass connection parameters as connect() arguments instead."
        )

    return f"{parsed.scheme}://{parsed.netloc}{parsed.path.rstrip('/')}"


def get_tls_verify(base_url: str, ssl_mode: str) -> Union[SSLContext, bool]:
    mode = normalize_ssl_mode(ssl_mode)
    if mode == SSL_MODE_NONE:
        return False
    if urlparse(base_url).scheme == "https":
        return get_core_certificate_context()
    return True


def build_discovery_url(base_url: str) -> str:
    return urljoin(base_url.rstrip("/") + "/", DISCOVERY_PATH.lstrip("/"))


def resolve_engine_name(
    engine: Optional[str],
    engine_name: Optional[str],
) -> Optional[str]:
    if engine and engine_name and engine != engine_name:
        raise ConfigurationError(
            "Both engine and engine_name are provided. Provide only one to connect."
        )
    return engine_name or engine


def validate_discovery_connection_parameters(
    account_name: Optional[str],
    api_endpoint: str,
    engine_url: Optional[str],
    url: Optional[str],
    auth: Optional[Auth],
) -> None:
    if account_name:
        raise ConfigurationError(
            "account_name is not compatible with discovery-based connections."
        )
    if api_endpoint != DEFAULT_API_URL:
        raise ConfigurationError(
            "api_endpoint is not compatible with discovery-based connections."
        )
    if engine_url:
        raise ConfigurationError(
            "engine_url is not compatible with discovery-based connections."
        )
    if url:
        raise ConfigurationError(
            "url is not compatible with discovery-based connections. Use host instead."
        )
    if auth and auth.get_firebolt_version() != FireboltAuthVersion.CORE:
        raise ConfigurationError(
            "auth is not compatible with discovery-based connections."
        )


def prepare_discovery_connection(
    auth: Optional[Auth],
    connection_id: str,
    additional_parameters: Dict[str, Any],
) -> PreparedDiscoveryConnection:
    core_auth = auth or FireboltCore()
    return PreparedDiscoveryConnection(
        auth=core_auth,
        user_agent_header=get_user_agent_for_connection(
            core_auth,
            connection_id,
            None,
            additional_parameters,
            True,
        ),
    )


def make_discovery_client_kwargs(
    discovery_info: DiscoveryConnectionInfo,
    prepared_connection: PreparedDiscoveryConnection,
) -> Dict[str, Any]:
    return {
        "auth": prepared_connection.auth,
        "account_name": "",
        "base_url": discovery_info.engine_url,
        "api_endpoint": discovery_info.api_endpoint,
        "timeout": Timeout(DEFAULT_TIMEOUT_SECONDS, read=None),
        "headers": {"User-Agent": prepared_connection.user_agent_header},
        "verify": discovery_info.verify,
    }


def _string_value(data: Mapping[str, Any], *keys: str) -> Optional[str]:
    for key in keys:
        value = data.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _endpoint_from_mapping(data: Mapping[str, Any]) -> Optional[str]:
    return _string_value(
        data,
        "engineUrl",
        "engine_url",
        "engineEndpoint",
        "engine_endpoint",
        "queryUrl",
        "query_url",
        "url",
        "endpoint",
    )


def _resolve_endpoint(base_url: str, endpoint: str) -> str:
    if "://" in endpoint or endpoint.startswith("/"):
        return urljoin(base_url.rstrip("/") + "/", endpoint)
    parsed_base = urlparse(base_url)
    if ":" in endpoint or "." in endpoint:
        return f"{parsed_base.scheme}://{endpoint}"
    return urljoin(base_url.rstrip("/") + "/", endpoint)


def _extract_engine_url(discovery: Mapping[str, Any], base_url: str) -> str:
    endpoint = _endpoint_from_mapping(discovery)
    if endpoint:
        return _resolve_endpoint(base_url, endpoint)

    endpoints = discovery.get("endpoints")
    if isinstance(endpoints, Mapping):
        for key in ("query", "sql", "engine", "http"):
            value = endpoints.get(key)
            if isinstance(value, str) and value:
                return _resolve_endpoint(base_url, value)
            if isinstance(value, Mapping):
                endpoint = _endpoint_from_mapping(value)
                if endpoint:
                    return _resolve_endpoint(base_url, endpoint)

    query = discovery.get("query")
    if isinstance(query, Mapping):
        endpoint = _endpoint_from_mapping(query)
        if endpoint:
            return _resolve_endpoint(base_url, endpoint)

    return base_url


def make_discovery_connection_info(
    host: str,
    ssl_mode: str,
    discovery: Mapping[str, Any],
    database: Optional[str] = None,
    engine: Optional[str] = None,
    engine_name: Optional[str] = None,
    settings: Optional[Dict[str, Any]] = None,
) -> DiscoveryConnectionInfo:
    base_url = normalize_host(host, ssl_mode)
    verify = get_tls_verify(base_url, ssl_mode)

    engine_parameter = resolve_engine_name(engine, engine_name)

    endpoint = _extract_engine_url(discovery, base_url)
    endpoint_url, endpoint_params = parse_url_and_params(endpoint)

    parameters: Dict[str, Any] = dict(endpoint_params)
    if settings:
        parameters.update(settings)
    if database:
        parameters["database"] = database
    if engine_parameter:
        parameters["engine"] = engine_parameter

    return DiscoveryConnectionInfo(
        engine_url=endpoint_url,
        api_endpoint=base_url,
        parameters=parameters,
        verify=verify,
    )


def _decode_discovery_response(response_text: str) -> Mapping[str, Any]:
    try:
        import json

        decoded = json.loads(response_text)
    except JSONDecodeError as e:
        raise InterfaceError("Unable to decode Firebolt discovery response.") from e
    if not isinstance(decoded, Mapping):
        raise InterfaceError("Firebolt discovery response must be a JSON object.")
    return decoded


def _raise_if_discovery_failed(status_code: int, text: str, discovery_url: str) -> None:
    if status_code != codes.OK:
        raise InterfaceError(
            f"Unable to retrieve Firebolt discovery document {discovery_url}: "
            f"{status_code} {text}"
        )


def _make_info_from_response(
    status_code: int,
    text: str,
    discovery_url: str,
    host: str,
    ssl_mode: str,
    database: Optional[str],
    engine: Optional[str],
    engine_name: Optional[str],
    settings: Optional[Dict[str, Any]],
) -> DiscoveryConnectionInfo:
    _raise_if_discovery_failed(status_code, text, discovery_url)
    return make_discovery_connection_info(
        host=host,
        ssl_mode=ssl_mode,
        discovery=_decode_discovery_response(text),
        database=database,
        engine=engine,
        engine_name=engine_name,
        settings=settings,
    )


def discover(
    host: str,
    ssl_mode: str,
    database: Optional[str] = None,
    engine: Optional[str] = None,
    engine_name: Optional[str] = None,
    settings: Optional[Dict[str, Any]] = None,
) -> DiscoveryConnectionInfo:
    base_url = normalize_host(host, ssl_mode)
    verify = get_tls_verify(base_url, ssl_mode)
    discovery_url = build_discovery_url(base_url)

    with HttpxClient(
        verify=verify,
        timeout=Timeout(DEFAULT_TIMEOUT_SECONDS),
    ) as client:
        response = client.get(discovery_url)

    return _make_info_from_response(
        status_code=response.status_code,
        text=response.text,
        discovery_url=discovery_url,
        host=host,
        ssl_mode=ssl_mode,
        database=database,
        engine=engine,
        engine_name=engine_name,
        settings=settings,
    )


async def async_discover(
    host: str,
    ssl_mode: str,
    database: Optional[str] = None,
    engine: Optional[str] = None,
    engine_name: Optional[str] = None,
    settings: Optional[Dict[str, Any]] = None,
) -> DiscoveryConnectionInfo:
    base_url = normalize_host(host, ssl_mode)
    verify = get_tls_verify(base_url, ssl_mode)
    discovery_url = build_discovery_url(base_url)

    async with HttpxAsyncClient(
        verify=verify,
        timeout=Timeout(DEFAULT_TIMEOUT_SECONDS),
    ) as client:
        response = await client.get(discovery_url)

    return _make_info_from_response(
        status_code=response.status_code,
        text=response.text,
        discovery_url=discovery_url,
        host=host,
        ssl_mode=ssl_mode,
        database=database,
        engine=engine,
        engine_name=engine_name,
        settings=settings,
    )
