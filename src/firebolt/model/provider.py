from functools import cached_property

from pydantic import Field

from firebolt.model import FireboltBaseModel, FireboltClientMixin


class Provider(FireboltBaseModel, frozen=True):  # type: ignore
    provider_id: str = Field(alias="id")
    name: str


class _Providers(FireboltClientMixin):
    @cached_property
    def providers(self) -> list[Provider]:
        response = self.get_firebolt_client().http_client.get(
            url="/compute/v1/providers", params={"page.first": 5000}
        )
        return [Provider.parse_obj(i["node"]) for i in response.json()["edges"]]

    @cached_property
    def providers_by_id(self) -> dict[str, Provider]:
        return {p.provider_id: p for p in self.providers}

    def get_by_id(self, provider_id: str) -> Provider:
        return self.providers_by_id[provider_id]

    @cached_property
    def providers_by_name(self) -> dict[str, Provider]:
        return {p.name: p for p in self.providers}

    def get_by_name(self, provider_name: str) -> Provider:
        return self.providers_by_name[provider_name]

    @cached_property
    def default_provider(self) -> Provider:
        return self.get_by_name(
            provider_name=self.get_firebolt_client().default_provider_name
        )


providers = _Providers()
