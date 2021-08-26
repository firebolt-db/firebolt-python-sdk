from datetime import datetime

from pydantic import BaseModel, Field

from firebolt.firebolt_client import get_firebolt_client
from firebolt.model.instance_type import InstanceTypeKey, instance_types


class EngineRevisionKey(BaseModel):
    account_id: str
    engine_id: str
    engine_revision_id: str


class Specification(BaseModel):
    db_compute_instances_type_id: InstanceTypeKey  # todo alias id to key?
    db_compute_instances_count: int
    db_compute_instances_use_spot: bool
    db_version: str
    proxy_instances_type_id: InstanceTypeKey  # todo alias id to key?
    proxy_instances_count: int
    proxy_version: str

    @classmethod
    def default_ingest(cls):
        instance_type_key = instance_types.get_by_name(instance_name="i3.4xlarge").key
        return (
            cls(
                db_compute_instances_type_id=instance_type_key,
                db_compute_instances_count=2,
                db_compute_instances_use_spot=False,
                db_version="",
                proxy_instances_type_id=instance_type_key,
                proxy_instances_count=1,
                proxy_version="",
            ),
        )


class EngineRevision(BaseModel):
    key: EngineRevisionKey = Field(alias="id")
    current_status: str
    specification: Specification
    create_time: datetime
    create_actor: str
    last_update_time: datetime
    last_update_actor: str
    desired_status: str
    health_status: str

    @classmethod
    def get_by_id(cls, engine_id: str, engine_revision_id: str):
        fc = get_firebolt_client()
        return cls.get_by_engine_revision_key(
            EngineRevisionKey(
                account_id=fc.account_id,
                engine_id=engine_id,
                engine_revision_id=engine_revision_id,
            )
        )

    @classmethod
    def get_by_engine_revision_key(cls, engine_revision_key: EngineRevisionKey):
        fc = get_firebolt_client()
        response = fc.http_client.get(
            url=f"/core/v1/accounts/{engine_revision_key.account_id}"
            f"/engines/{engine_revision_key.engine_id}"
            f"/engineRevisions/{engine_revision_key.engine_revision_id}",
        )
        engine_spec: dict = response.json()["engine_revision"]
        return cls.parse_obj(engine_spec)
