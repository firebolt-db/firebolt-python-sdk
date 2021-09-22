from pydantic import BaseModel

from firebolt.client import FireboltClient, get_firebolt_client


class FireboltClientMixin:
    # FUTURE: it would be nice to make this a property also, once PyCharm supports it
    # https://youtrack.jetbrains.com/issue/PY-47615
    @classmethod
    def get_firebolt_client(cls) -> FireboltClient:
        return get_firebolt_client()


class FireboltBaseModel(BaseModel, FireboltClientMixin):
    class Config:
        allow_population_by_field_name = True
        extra = "forbid"
