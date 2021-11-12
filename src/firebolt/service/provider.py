from firebolt.client import Client
from firebolt.common.urls import PROVIDERS_URL
from firebolt.model.provider import Provider


def get_provider_id(client: Client) -> str:
    """Get the AWS provider_id."""
    response = client.get(url=PROVIDERS_URL)
    providers = [Provider.parse_obj(i["node"]) for i in response.json()["edges"]]
    return providers[0].provider_id
