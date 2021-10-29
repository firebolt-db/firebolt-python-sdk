from typing import Dict, Generator, List

from httpx import Client, Request, Response

from firebolt.model import FireboltBaseModel


def list_to_paginated_response(items: List[FireboltBaseModel]) -> Dict:
    return {"edges": [{"node": i.dict()} for i in items]}


def execute_generator_requests(requests: Generator[Request, Response, None]) -> None:
    request = next(requests)

    with Client() as client:
        while True:
            response = client.send(request)
            try:
                request = requests.send(response)
            except StopIteration:
                break
