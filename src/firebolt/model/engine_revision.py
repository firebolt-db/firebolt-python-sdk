from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import Field, PositiveInt

from firebolt.model import FireboltBaseModel
from firebolt.model.instance_type import InstanceTypeKey


class EngineRevisionKey(FireboltBaseModel):
    account_id: str
    engine_id: str
    engine_revision_id: str


class EngineRevisionSpecification(FireboltBaseModel):
    """
    An EngineRevision specification.

    Determines which instance types and how many of them its engine gets.

    See Also: :py:class:`Settings
    <firebolt.model.engine.EngineSettings>`,
    which also contains engine configuration.
    """

    db_compute_instances_type_key: InstanceTypeKey = Field(
        alias="db_compute_instances_type_id"
    )
    db_compute_instances_count: PositiveInt
    db_compute_instances_use_spot: bool = False
    db_version: str = ""
    proxy_instances_type_key: InstanceTypeKey = Field(alias="proxy_instances_type_id")
    proxy_instances_count: PositiveInt = 1
    proxy_version: str = ""


class EngineRevision(FireboltBaseModel):
    """
    A Firebolt engine revision,
    which contains a specification (instance types, counts).

    As engines are updated with new settings, revisions are created.
    """

    specification: EngineRevisionSpecification

    # optional
    key: Optional[EngineRevisionKey] = Field(alias="id")
    current_status: Optional[str]
    create_time: Optional[datetime]
    create_actor: Optional[str]
    last_update_time: Optional[datetime]
    last_update_actor: Optional[str]
    desired_status: Optional[str]
    health_status: Optional[str]
