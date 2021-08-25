import logging
import time
from typing import Any

from pydantic import BaseModel

from firebolt.firebolt_client import get_firebolt_client
from firebolt.model.engine_revision import EngineRevision, EngineRevisionKey

logger = logging.getLogger(__name__)


class EngineId(BaseModel):
    account_id: str
    engine_id: str


class ComputeRegionId(BaseModel):
    provider_id: str
    region_id: str

    @classmethod
    def us_east_1(cls):
        return cls(
            provider_id="402a51bb-1c8e-4dc4-9e05-ced3c1e2186e",
            region_id="f1841f9f-4031-4a9a-b3d7-1dc27e7e61ed",
        )

    @classmethod
    def eu_west_1(cls):
        return cls(
            provider_id="402a51bb-1c8e-4dc4-9e05-ced3c1e2186e",
            region_id="fcacdb84-5206-4f5c-99b5-75668e1f53fb",
        )


class Settings(BaseModel):
    preset: str
    auto_stop_delay_duration: str
    minimum_logging_level: str
    is_read_only: bool
    warm_up: str

    @classmethod
    def analytics_default(cls):
        return cls(
            preset="ENGINE_SETTINGS_PRESET_DATA_ANALYTICS",
            auto_stop_delay_duration="1200s",
            minimum_logging_level="ENGINE_SETTINGS_LOGGING_LEVEL_INFO",
            is_read_only=True,
            warm_up="ENGINE_SETTINGS_WARM_UP_INDEXES",
        )

    @classmethod
    def ingest_default(cls):
        return cls(
            preset="ENGINE_SETTINGS_PRESET_GENERAL_PURPOSE",
            auto_stop_delay_duration="1200s",
            minimum_logging_level="ENGINE_SETTINGS_LOGGING_LEVEL_INFO",
            is_read_only=False,
            warm_up="ENGINE_SETTINGS_WARM_UP_INDEXES",
        )


# class RevisionId(BaseModel):
#     account_id: str
#     engine_id: str
#     engine_revision_id: str


class Engine(BaseModel):
    id: EngineId
    name: str
    description: str
    emoji: str
    compute_region_id: ComputeRegionId
    settings: Settings
    current_status: str
    current_status_summary: str
    latest_revision_id: EngineRevisionKey
    endpoint: str
    endpoint_serving_revision_id: Any
    create_time: str
    create_actor: str
    last_update_time: str
    last_update_actor: str
    last_use_time: Any
    desired_status: str
    health_status: str
    endpoint_desired_revision_id: Any

    @property
    def engine_id(self) -> str:
        return self.id.engine_id

    @classmethod
    def get_by_id(cls, engine_id: str):
        fc = get_firebolt_client()
        response = fc.http_client.get(
            url=f"/core/v1/accounts/{fc.account_id}/engines/{engine_id}",
        )
        engine_spec: dict = response.json()["engine"]
        return cls.parse_obj(engine_spec)

    @classmethod
    def get_by_name(cls, engine_name: str):
        fc = get_firebolt_client()
        response = fc.http_client.get(
            url=f"/core/v1/account/engines:getIdByName",
            params={"engine_name": engine_name},
        )
        engine_id = response.json()["engine_id"]["engine_id"]
        return cls.get_by_id(engine_id=engine_id)

    @classmethod
    def create_engine(cls):
        pass

    def get_latest_engine_revision(self):
        return EngineRevision.get_by_engine_revision_key(
            engine_revision_key=self.latest_revision_id
        )

    def start(
        self,
        wait_for_startup: bool = True,
        wait_timeout_seconds: int = 3600,
        print_dots=True,
    ):
        fc = get_firebolt_client()
        response = fc.http_client.post(
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
