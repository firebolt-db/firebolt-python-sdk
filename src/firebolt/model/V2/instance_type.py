from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict

from firebolt.model.V2 import FireboltBaseModel


@dataclass
class InstanceType(FireboltBaseModel):
    _key: Dict = field(repr=False, metadata={"db_name": "id"})
    name: str = field()
    is_spot_available: bool = field()
    cpu_virtual_cores_count: int = field()
    memory_size_bytes: int = field()
    storage_size_bytes: int = field()
    price_per_hour_cents: float = field()
    create_time: datetime = field()
    last_update_time: datetime = field()

    def __str__(self) -> str:
        return self.name
