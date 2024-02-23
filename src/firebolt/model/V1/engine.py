from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import TYPE_CHECKING, Any, Optional, Sequence

from pydantic import Field, PrivateAttr

from firebolt.model.V1 import FireboltBaseModel
from firebolt.model.V1.engine_revision import EngineRevisionKey
from firebolt.model.V1.region import RegionKey
from firebolt.service.V1.types import (
    EngineStatus,
    EngineStatusSummary,
    EngineType,
    WarmupMethod,
)
from firebolt.utils.urls import (
    ACCOUNT_ENGINE_START_URL,
    ACCOUNT_ENGINE_STOP_URL,
)

if TYPE_CHECKING:
    from firebolt.service.V1.engine import EngineService

logger = logging.getLogger(__name__)


class EngineKey(FireboltBaseModel):
    account_id: str
    engine_id: str


def wait(seconds: int, timeout_time: float, error_message: str, verbose: bool) -> None:
    time.sleep(seconds)
    if time.time() > timeout_time:
        raise TimeoutError(error_message)
    if verbose:
        print(".", end="")


class EngineSettings(FireboltBaseModel):
    """
    Engine settings.

    See also: :py:class:`EngineRevisionSpecification
    <firebolt.model.engine_revision.EngineRevisionSpecification>`
    which also contains engine configuration.
    """

    preset: str
    auto_stop_delay_duration: str = Field(pattern=r"^[0-9]+[sm]$|^0$")
    minimum_logging_level: str
    is_read_only: bool
    warm_up: str

    @classmethod
    def default(
        cls,
        engine_type: EngineType = EngineType.GENERAL_PURPOSE,
        auto_stop_delay_duration: str = "1200s",
        warm_up: WarmupMethod = WarmupMethod.PRELOAD_INDEXES,
        minimum_logging_level: str = "ENGINE_SETTINGS_LOGGING_LEVEL_INFO",
    ) -> EngineSettings:
        if engine_type == EngineType.GENERAL_PURPOSE:
            preset = engine_type.GENERAL_PURPOSE.api_settings_preset_name  # type: ignore # noqa: E501
            is_read_only = False
        else:
            preset = engine_type.DATA_ANALYTICS.api_settings_preset_name  # type: ignore
            is_read_only = True

        return cls(
            preset=preset,
            auto_stop_delay_duration=auto_stop_delay_duration,
            minimum_logging_level=minimum_logging_level,
            is_read_only=is_read_only,
            warm_up=warm_up.api_name,
        )


class FieldMask(FireboltBaseModel):
    paths: Sequence[str] = Field(alias="paths")


class Engine(FireboltBaseModel):
    """
    A Firebolt engine. Responsible for performing work (queries, ingestion).

    Engines are configured in :py:class:`Settings
    <firebolt.model.engine.EngineSettings>`
    and in :py:class:`EngineRevisionSpecification
    <firebolt.model.engine_revision.EngineRevisionSpecification>`.
    """

    # internal
    _service: EngineService = PrivateAttr()

    # required
    name: str = Field(min_length=1, max_length=255, pattern=r"^[0-9a-zA-Z_]+$")
    compute_region_key: RegionKey = Field(alias="compute_region_id")
    settings: EngineSettings

    # optional
    key: Optional[EngineKey] = Field(None, alias="id")
    description: Optional[str] = None
    emoji: Optional[str] = None
    current_status: Optional[EngineStatus] = None
    current_status_summary: Optional[EngineStatusSummary] = None
    latest_revision_key: Optional[EngineRevisionKey] = Field(
        None, alias="latest_revision_id"
    )
    endpoint: Optional[str] = None
    endpoint_serving_revision_key: Optional[EngineRevisionKey] = Field(
        None, alias="endpoint_serving_revision_id"
    )
    create_time: Optional[datetime] = None
    create_actor: Optional[str] = None
    last_update_time: Optional[datetime] = None
    last_update_actor: Optional[str] = None
    last_use_time: Optional[datetime] = None
    desired_status: Optional[str] = None
    health_status: Optional[str] = None
    endpoint_desired_revision_key: Optional[EngineRevisionKey] = Field(
        None, alias="endpoint_desired_revision_id"
    )

    @classmethod
    def parse_obj_with_service(cls, obj: Any, engine_service: EngineService) -> Engine:
        engine = cls.parse_model(obj)
        engine._service = engine_service
        return engine

    @property
    def engine_id(self) -> str:
        if self.key is None:
            raise ValueError("engine key is None")
        return self.key.engine_id

    def get_latest(self) -> Engine:
        """Get an up-to-date instance of the engine from Firebolt."""
        return self._service.get(id_=self.engine_id)

    def start(
        self,
        wait_for_startup: bool = True,
        wait_timeout_seconds: int = 3600,
        verbose: bool = False,
    ) -> Engine:
        """
        Start an engine. If it's already started, do nothing.

        Args:
            wait_for_startup:
                If True, wait for startup to complete.
                If False, return immediately after requesting startup.
            wait_timeout_seconds:
                Number of seconds to wait for startup to complete
                before raising a TimeoutError
            verbose:
                If True, print dots periodically while waiting for engine start.
                If False, do not print any dots.

        Returns:
            The updated engine from Firebolt.
        """
        timeout_time = time.time() + wait_timeout_seconds

        engine = self.get_latest()
        if (
            engine.current_status_summary
            == EngineStatusSummary.ENGINE_STATUS_SUMMARY_RUNNING
        ):
            logger.info(
                f"Engine (engine_id={self.engine_id}, name={self.name}) "
                "is already running."
            )
            return engine

        # wait for engine to stop first, if it's already stopping
        # FUTURE: revisit logging and consider consolidating this if & the while below.
        elif (
            engine.current_status_summary
            == EngineStatusSummary.ENGINE_STATUS_SUMMARY_STOPPING
        ):
            logger.info(
                f"Engine (engine_id={engine.engine_id}, name={engine.name}) "
                "is in currently stopping, waiting for it to stop first."
            )
            while (
                engine.current_status_summary
                != EngineStatusSummary.ENGINE_STATUS_SUMMARY_STOPPED
            ):
                wait(
                    seconds=5,
                    timeout_time=timeout_time,
                    error_message=(
                        "Engine "
                        f"(engine_id={engine.engine_id}, name={engine.name}) "
                        f"did not stop within {wait_timeout_seconds} seconds."
                    ),
                    verbose=True,
                )
                engine = engine.get_latest()

            logger.info(
                f"Engine (engine_id={engine.engine_id}, name={engine.name}) stopped."
            )

        engine = self._send_engine_request(ACCOUNT_ENGINE_START_URL)
        logger.info(
            f"Starting Engine (engine_id={engine.engine_id}, name={engine.name})"
        )

        # wait for engine to start
        while wait_for_startup and engine.current_status_summary not in {
            EngineStatusSummary.ENGINE_STATUS_SUMMARY_RUNNING,
            EngineStatusSummary.ENGINE_STATUS_SUMMARY_FAILED,
        }:
            wait(
                seconds=5,
                timeout_time=timeout_time,
                error_message=(  # noqa: E501
                    f"Could not start engine within {wait_timeout_seconds} seconds."
                ),
                verbose=verbose,
            )
            previous_status_summary = engine.current_status_summary
            engine = engine.get_latest()
            if engine.current_status_summary != previous_status_summary:
                logger.info(
                    "Engine status_summary="
                    f"{getattr(engine.current_status_summary, 'name')}"
                )

        return engine

    def stop(
        self, wait_for_stop: bool = False, wait_timeout_seconds: int = 3600
    ) -> Engine:
        """Stop an Engine running on Firebolt."""
        timeout_time = time.time() + wait_timeout_seconds

        engine = self._send_engine_request(ACCOUNT_ENGINE_STOP_URL)
        logger.info(f"Stopping Engine (engine_id={self.engine_id}, name={self.name})")

        while wait_for_stop and engine.current_status_summary not in {
            EngineStatusSummary.ENGINE_STATUS_SUMMARY_STOPPED,
            EngineStatusSummary.ENGINE_STATUS_SUMMARY_FAILED,
        }:
            wait(
                seconds=5,
                timeout_time=timeout_time,
                error_message=(  # noqa: E501
                    f"Could not stop engine within {wait_timeout_seconds} seconds."
                ),
                verbose=False,
            )

            engine = engine.get_latest()

        return engine

    def _send_engine_request(self, url: str) -> Engine:
        response = self._service.client.post(
            url=url.format(
                account_id=self._service.account_id, engine_id=self.engine_id
            )
        )
        return Engine.parse_obj_with_service(
            obj=response.json()["engine"], engine_service=self._service
        )
