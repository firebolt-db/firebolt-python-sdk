import json
from dataclasses import dataclass, field, fields
from typing import ClassVar, Dict, Iterable, Optional, Type, TypeVar

from firebolt.service.V2.base import BaseService

Model = TypeVar("Model", bound="FireboltBaseModel")


@dataclass
class FireboltBaseModel:
    _service: BaseService = field(repr=False, compare=False)

    @classmethod
    def _get_field_overrides(cls) -> Dict[str, str]:
        """Create a mapping of db field name to class name where they are different."""
        return {
            override: f.name
            for f in fields(cls)
            if "db_name" in f.metadata
            for override in (
                [f.metadata["db_name"]]
                if isinstance(f.metadata["db_name"], str)
                else f.metadata["db_name"]
            )
        }

    @classmethod
    def _from_dict(
        cls: Type[Model], data: dict, service: Optional[BaseService] = None
    ) -> Model:
        data["_service"] = service
        field_name_overrides = cls._get_field_overrides()
        return cls(**{field_name_overrides.get(k, k): v for k, v in data.items()})
