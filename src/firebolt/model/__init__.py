from pydantic import BaseModel


class FireboltBaseModel(BaseModel):
    class Config:
        allow_population_by_field_name = True
        extra = "forbid"
