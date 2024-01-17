from datetime import datetime
from typing import Optional

from pydantic import Field

from firebolt.model.V1 import FireboltBaseModel


class Provider(FireboltBaseModel, frozen=True):  # type: ignore
    provider_id: str = Field(alias="id")
    name: str

    # optional
    create_time: Optional[datetime] = None
    display_name: Optional[str] = None
    last_update_time: Optional[datetime] = None
