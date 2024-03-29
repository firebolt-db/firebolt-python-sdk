from dataclasses import Field, dataclass, fields
from typing import AsyncGenerator, Dict, Generator, List

from httpx import Request, Response

from firebolt.client import AsyncClientV2 as AsyncClient
from firebolt.client import ClientV2
from firebolt.client.auth import Auth
from firebolt.model.V1 import FireboltBaseModel as FireboltBaseModelV1
from firebolt.model.V2 import FireboltBaseModel


def field_name(f: Field) -> str:
    return (f.metadata or {}).get("db_name", f.name)


def to_dict(dc: dataclass) -> Dict:
    return {field_name(f): getattr(dc, f.name) for f in fields(dc)}


def list_to_paginated_response(items: List[FireboltBaseModel]) -> Dict:
    return {"edges": [{"node": to_dict(i)} for i in items]}


def list_to_paginated_response_v1(items: List[FireboltBaseModelV1]) -> Dict:
    return {"edges": [{"node": i.model_dict()} for i in items]}


def execute_generator_requests(
    requests: Generator[Request, Response, None], api_endpoint: str = ""
) -> None:
    request = next(requests)

    with ClientV2(
        account_name="account", auth=Auth(), api_endpoint=api_endpoint
    ) as client:
        client._auth = None
        while True:
            response = client.send(request)
            try:
                request = requests.send(response)
            except StopIteration:
                break


async def async_execute_generator_requests(
    requests: AsyncGenerator[Request, Response],
    api_endpoint: str = "",
) -> None:
    request = await requests.__anext__()

    async with AsyncClient(
        account_name="account", auth=Auth(), api_endpoint=api_endpoint
    ) as client:
        client._auth = None
        while True:
            response = await client.send(request)
            try:
                request = await requests.asend(response)
            except StopAsyncIteration:
                break
