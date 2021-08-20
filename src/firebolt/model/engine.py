from typing import Any

from pydantic import BaseModel


class EngineId(BaseModel):
    account_id: str
    engine_id: str


class ComputeRegionId(BaseModel):
    provider_id: str
    region_id: str


class Settings(BaseModel):
    preset: str
    auto_stop_delay_duration: str
    minimum_logging_level: str
    is_read_only: bool
    warm_up: str


class RevisionId(BaseModel):
    account_id: str
    engine_id: str
    engine_revision_id: str


class Engine(BaseModel):
    id: EngineId
    name: str
    description: str
    emoji: str
    compute_region_id: ComputeRegionId
    settings: Settings
    current_status: str
    current_status_summary: str
    latest_revision_id: RevisionId
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
