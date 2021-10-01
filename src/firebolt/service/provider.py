from functools import cached_property

from firebolt.model.provider import Provider
from firebolt.service.base import BaseService


class ProviderService(BaseService):
    @cached_property
    def providers(self) -> list[Provider]:
        """List of available Providers on Firebolt"""
        response = self.client.get(
            url="/compute/v1/providers", params={"page.first": 5000}
        )
        return [Provider.parse_obj(i["node"]) for i in response.json()["edges"]]

    @cached_property
    def providers_by_id(self) -> dict[str, Provider]:
        """Dict of {provider_id: Provider}"""
        return {p.provider_id: p for p in self.providers}

    @cached_property
    def providers_by_name(self) -> dict[str, Provider]:
        """Dict of {provider_name: Provider}"""
        return {p.name: p for p in self.providers}

    @cached_property
    def default_provider(self) -> Provider:
        """The default Provider as specified by the client"""
        return self.get_by_name(provider_name="AWS")

    def get_by_id(self, provider_id: str) -> Provider:
        """Get a Provider by its id"""
        return self.providers_by_id[provider_id]

    def get_by_name(self, provider_name: str) -> Provider:
        """Get a Provider by its name"""
        return self.providers_by_name[provider_name]
