from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field
from toolz import first

from firebolt.firebolt_client import FireboltClientMixin
from firebolt.model.binding import Binding
from firebolt.model.database import Database
from firebolt.model.engine_revision import EngineRevision, EngineRevisionKey
from firebolt.model.region import RegionKey, regions

logger = logging.getLogger(__name__)


class EngineKey(BaseModel):
    account_id: str
    engine_id: str


class Settings(BaseModel):
    preset: str
    auto_stop_delay_duration: str
    minimum_logging_level: str
    is_read_only: bool
    warm_up: str

    @classmethod
    def analytics_default(cls) -> Settings:
        return cls(
            preset="ENGINE_SETTINGS_PRESET_DATA_ANALYTICS",
            auto_stop_delay_duration="1200s",
            minimum_logging_level="ENGINE_SETTINGS_LOGGING_LEVEL_INFO",
            is_read_only=True,
            warm_up="ENGINE_SETTINGS_WARM_UP_INDEXES",
        )

    @classmethod
    def ingest_default(cls) -> Settings:
        return cls(
            preset="ENGINE_SETTINGS_PRESET_GENERAL_PURPOSE",
            auto_stop_delay_duration="1200s",
            minimum_logging_level="ENGINE_SETTINGS_LOGGING_LEVEL_INFO",
            is_read_only=False,
            warm_up="ENGINE_SETTINGS_WARM_UP_INDEXES",
        )


class Engine(BaseModel, FireboltClientMixin):
    key: Optional[EngineKey] = Field(alias="id")
    name: str
    description: Optional[str]
    emoji: Optional[str]
    compute_region_key: RegionKey = Field(alias="compute_region_id")
    settings: Settings
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

    class Config:
        allow_population_by_field_name = True

    @property
    def engine_id(self) -> Optional[str]:
        if self.key is None:
            return None
        return self.key.engine_id

    @classmethod
    def get_by_id(cls, engine_id: str):
        fc = cls.get_firebolt_client()
        response = fc.http_client.get(
            url=f"/core/v1/accounts/{fc.account_id}/engines/{engine_id}",
        )
        engine_spec: dict = response.json()["engine"]
        return cls.parse_obj(engine_spec)

    @classmethod
    def get_by_ids(cls, engine_ids: list[str]) -> list[Engine]:
        fc = cls.get_firebolt_client()
        response = fc.http_client.post(
            url=f"/core/v1/engines:getByIds",
            json={
                "engine_ids": [
                    {"account_id": fc.account_id, "engine_id": engine_id}
                    for engine_id in engine_ids
                ]
            },
        )
        return [cls.parse_obj(e) for e in response.json()["engines"]]

    @classmethod
    def get_by_name(cls, engine_name: str):
        response = cls.get_firebolt_client().http_client.get(
            url="/core/v1/account/engines:getIdByName",
            params={"engine_name": engine_name},
        )
        engine_id = response.json()["engine_id"]["engine_id"]
        return cls.get_by_id(engine_id=engine_id)

    @classmethod
    def list_engines(cls) -> list[Engine]:
        fc = cls.get_firebolt_client()
        response = fc.http_client.get(
            url=f"/core/v1/accounts/{fc.account_id}/engines",
            params={"page.first": 5000},  # FUTURE: consider generator
        )
        return [cls.parse_obj(i["node"]) for i in response.json()["edges"]]

    def delete(self):
        response = self.firebolt_client.http_client.delete(
            url=f"/core/v1/accounts/{self.firebolt_client.account_id}/engines/{self.engine_id}",
        )
        return response.json()

    @classmethod
    def analytics_default(
        cls,
        name: str,
        description: Optional[str] = None,
        region_name: Optional[str] = None,
    ) -> Engine:
        if region_name is not None:
            region = regions.get_by_name(region_name=region_name)
        else:
            region = regions.default_region
        return Engine(
            name=name,
            description=description,
            compute_region_key=region.key,
            settings=Settings.analytics_default(),
        )

    @property
    def database(self) -> Optional[Database]:
        # FUTURE: in the new architecture, an engine can be bound to multiple databases
        try:
            binding = first(Binding.list_bindings(engine_id=self.engine_id))
            return Database.get_by_id(binding.database_id)
        except StopIteration:
            return None

    def create(self, engine_revision: Optional[EngineRevision] = None):
        if engine_revision is None:
            engine_revision = self.get_latest_engine_revision()

        json_payload = EngineCreate(
            account_id=self.firebolt_client.account_id,
            engine=self,
            engine_revision=engine_revision,
        ).json(by_alias=True)

        response = self.firebolt_client.http_client.post(
            url="/core/v1/account/engines",
            headers={"Content-type": "application/json"},
            data=json_payload,
        )
        return response.json()

    def get_latest_engine_revision(self) -> Optional[EngineRevision]:
        if self.latest_revision_key is None:
            return None
        return EngineRevision.get_by_engine_revision_key(
            engine_revision_key=self.latest_revision_key
        )

    def start(
        self,
        wait_for_startup: bool = True,
        wait_timeout_seconds: int = 3600,
        print_dots=True,
    ):
        if self.engine_id is None:
            raise ValueError("engine_id must be set before starting")
        response = self.firebolt_client.http_client.post(
            url=f"/core/v1/account/engines/{self.engine_id}:start",
        )
        status = response.json()["engine"]["current_status_summary"]
        logger.info(
            f"Starting Engine engine_id={self.engine_id} name={self.name} status_summary={status}"
        )
        start_time = time.time()
        end_time = start_time + wait_timeout_seconds
        while (
            wait_for_startup
            and status
            != "ENGINE_STATUS_SUMMARY_RUNNING"  # summary statuses: https://tinyurl.com/as7a9ru9
        ):
            if time.time() >= end_time:
                raise TimeoutError(
                    f"Could not start engine within {wait_timeout_seconds} seconds."
                )
            new_status = self.get_by_id(engine_id=self.engine_id).current_status_summary
            if new_status != status:
                logger.info(f"Engine status_summary={new_status}")
            elif print_dots:
                print(".", end="")
            time.sleep(5)
            status = new_status


class EngineCreate(BaseModel):
    """Helper model for sending Engine create requests"""

    account_id: str
    engine: Engine
    engine_revision: Optional[EngineRevision]
