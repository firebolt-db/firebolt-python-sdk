from datetime import datetime

from pydantic import BaseModel


class EngineRevisionId(BaseModel):
    account_id: str
    engine_id: str
    engine_revision_id: str


class DbComputeInstancesTypeId(BaseModel):
    provider_id: str
    region_id: str
    instance_type_id: str


class ProxyInstancesTypeId(BaseModel):
    provider_id: str
    region_id: str
    instance_type_id: str


class Specification(BaseModel):
    db_compute_instances_type_id: DbComputeInstancesTypeId
    db_compute_instances_count: int
    db_compute_instances_use_spot: bool
    db_version: str
    proxy_instances_type_id: ProxyInstancesTypeId
    proxy_instances_count: int
    proxy_version: str


class EngineRevision(BaseModel):
    id: EngineRevisionId
    current_status: str
    specification: Specification
    create_time: datetime
    create_actor: str
    last_update_time: datetime
    last_update_actor: str
    desired_status: str
    health_status: str


class Model(BaseModel):
    engine_revision: EngineRevision
