from datetime import datetime
from typing import Optional

from pydantic import Field

from firebolt.model import FireboltBaseModel


class Provider(FireboltBaseModel, frozen=True):  # type: ignore
    provider_id: str = Field(alias="id")
    name: str

    # optional
    create_time: Optional[datetime]
    display_name: Optional[str]
    last_update_time: Optional[datetime]
