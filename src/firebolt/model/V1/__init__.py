import json
from typing import Any, Callable, Final, Type, TypeVar, Union

import pydantic
from typing_extensions import TypeAlias

Model = TypeVar("Model", bound="pydantic.BaseModel")

GenericCallable: TypeAlias = Callable[..., Any]

# Using `.VERSION` instead of `.__version__` for backward compatibility:
PYDANTIC_VERSION: Final[int] = int(pydantic.VERSION[0])


def use_if_version_ge(
    version_ge: int,
    obj: Union[pydantic.BaseModel, Type[Model]],
    previous_method: str,
    latest_method: str,
) -> GenericCallable:
    """
    Utility function to get desired method from base model.

    Args:
        version_ge: The version number that will be used to determine
            the desired method.
        obj: The object on which the method will be taken from
        previous_method: The method previously available in a version
            smaller than `version_ge`.
        latest_method:  The method available from `version_ge` onwards.

    """
    if PYDANTIC_VERSION >= version_ge:
        return getattr(obj, latest_method)
    else:
        return getattr(obj, previous_method)


if PYDANTIC_VERSION >= 2:
    # This import can only happen outside the BaseModel,
    # or it will raise PydanticUserError
    from pydantic import ConfigDict


class FireboltBaseModel(pydantic.BaseModel):
    if PYDANTIC_VERSION >= 2:
        # Pydantic V2 config
        model_config = ConfigDict(populate_by_name=True, from_attributes=True)

    else:
        # Using Pydantic 1.* config class for backwards compatibility
        class Config:
            extra = "forbid"
            allow_population_by_field_name = True  # Pydantic 1.8

    def model_dict(self, *args: Any, **kwargs: Any) -> dict:
        """Pydantic V2 and V1 compatible method for `dict` -> `model_dump`."""
        return use_if_version_ge(2, self, "dict", "model_dump")(*args, **kwargs)

    @classmethod
    def parse_model(cls: Type[Model], *args: Any, **kwargs: Any) -> Model:
        """Pydantic V2 and V1 compatible method for `parse_obj` -> `model_validate`."""
        return use_if_version_ge(2, cls, "parse_obj", "model_validate")(*args, **kwargs)

    def model_json(self, *args: Any, **kwargs: Any) -> str:
        """Pydantic V2 and V1 compatible method for `json` -> `model_dump_json`."""
        return use_if_version_ge(2, self, "json", "model_dump_json")(*args, **kwargs)

    def jsonable_dict(self, *args: Any, **kwargs: Any) -> dict:
        """
        Generate a dictionary representation of the service that contains serialized
        primitive types, and is therefore JSON-ready.

        This could be replaced with something native once this issue is resolved:
        https://github.com/samuelcolvin/pydantic/issues/1409

        This function is intended to improve the compatibility with HTTPX, which
        expects to take in a dictionary of primitives as input to the JSON parameter
        of its request function. See: https://www.python-httpx.org/api/#helper-functions
        """
        return json.loads(self.model_json(*args, **kwargs))
