from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from firebolt.model import FireboltBaseModel


@dataclass
class Provider(FireboltBaseModel):
    provider_id: str = field(metadata={"db_name": "id"})
    name: str = field()

    # optional
    create_time: Optional[datetime] = field(default=None)
    display_name: Optional[str] = field(default=None)
    last_update_time: Optional[datetime] = field(default=None)
