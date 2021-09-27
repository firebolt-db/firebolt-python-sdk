from __future__ import annotations

import logging
from datetime import datetime
from typing import Annotated, Optional

from pydantic import Field

from firebolt.model import FireboltBaseModel
from firebolt.model.engine_revision import EngineRevisionKey
from firebolt.model.region import RegionKey

logger = logging.getLogger(__name__)


class EngineKey(FireboltBaseModel):
    account_id: str
    engine_id: str


class EngineSettings(FireboltBaseModel):
    """
    Engine Settings.

    See Also: engine_revision.Specification which also contains engine configuration.
    """

    preset: str
    auto_stop_delay_duration: str
    minimum_logging_level: str
    is_read_only: bool
    warm_up: str

    @classmethod
    def analytics_default(cls) -> EngineSettings:
        """Default settings for the data analytics (querying) use case."""
        return cls(
            preset="ENGINE_SETTINGS_PRESET_DATA_ANALYTICS",
            auto_stop_delay_duration="1200s",
            minimum_logging_level="ENGINE_SETTINGS_LOGGING_LEVEL_INFO",
            is_read_only=True,
            warm_up="ENGINE_SETTINGS_WARM_UP_INDEXES",
        )

    @classmethod
    def general_purpose_default(cls) -> EngineSettings:
        """Default settings for the general purpose (data ingestion) use case."""
        return cls(
            preset="ENGINE_SETTINGS_PRESET_GENERAL_PURPOSE",
            auto_stop_delay_duration="1200s",
            minimum_logging_level="ENGINE_SETTINGS_LOGGING_LEVEL_INFO",
            is_read_only=False,
            warm_up="ENGINE_SETTINGS_WARM_UP_INDEXES",
        )


class Engine(FireboltBaseModel):
    """
    A Firebolt engine. Responsible for performing work (queries, data ingestion).

    Engines are configured in Settings and in EngineRevisions.
    """

    name: Annotated[str, Field(min_length=1, max_length=255, regex=r"^[0-9a-zA-Z_]+$")]
    compute_region_key: RegionKey = Field(alias="compute_region_id")
    settings: EngineSettings

    # optional
    key: Optional[EngineKey] = Field(alias="id")
    description: Optional[str]
    emoji: Optional[str]
    current_status: Optional[str]
    current_status_summary: Optional[str]
    latest_revision_key: Optional[EngineRevisionKey] = Field(alias="latest_revision_id")
    endpoint: Optional[str]
    endpoint_serving_revision_key: Optional[EngineRevisionKey] = Field(
        alias="endpoint_serving_revision_id"
    )
    create_time: Optional[datetime]
    create_actor: Optional[str]
    last_update_time: Optional[datetime]
    last_update_actor: Optional[str]
    last_use_time: Optional[datetime]
    desired_status: Optional[str]
    health_status: Optional[str]
    endpoint_desired_revision_key: Optional[EngineRevisionKey] = Field(
        alias="endpoint_desired_revision_id"
    )

    @property
    def engine_id(self) -> str:
        if self.key is None:
            raise ValueError("engine key is None")
        return self.key.engine_id
