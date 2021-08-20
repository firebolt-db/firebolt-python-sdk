from datetime import datetime

from pydantic import BaseModel


class DatabaseId(BaseModel):
    account_id: str
    database_id: str


class ComputeRegionId(BaseModel):
    provider_id: str
    region_id: str


class Database(BaseModel):
    id: DatabaseId
    name: str
    description: str
    emoji: str
    compute_region_id: ComputeRegionId
    current_status: str
    health_status: str
    data_size_full: int
    data_size_compressed: int
    is_system_database: bool
    storage_bucket_name: str
    create_time: datetime
    create_actor: str
    last_update_time: datetime
    last_update_actor: str
    desired_status: str
