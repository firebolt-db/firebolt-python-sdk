from firebolt.client import Client
from firebolt.model.V1.provider import Provider
from firebolt.utils.urls import PROVIDERS_URL


def get_provider_id(client: Client) -> str:
    """Get the AWS provider_id."""
    response = client.get(url=PROVIDERS_URL)
    providers = [Provider.parse_model(i["node"]) for i in response.json()["edges"]]
    return providers[0].provider_id
