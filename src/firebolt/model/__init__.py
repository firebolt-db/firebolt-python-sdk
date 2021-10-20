import json
from typing import Any

from pydantic import BaseModel


class FireboltBaseModel(BaseModel):
    class Config:
        allow_population_by_field_name = True
        extra = "forbid"

    def jsonable_dict(self, *args: Any, **kwargs: Any) -> dict:
        """
        Generate a dictionary representation of the service that is contains serialized
        primitive types, and is therefore json-ready.

        This could be replaced with something native, once this issue is resolved:
        https://github.com/samuelcolvin/pydantic/issues/1409

        This function is intended to improve the compatibility with httpx, which
        expects to take in a dictionary of primitives as input to the json parameter
        of its request function. See: https://www.python-httpx.org/api/#helper-functions
        """
        return json.loads(self.json(*args, **kwargs))
