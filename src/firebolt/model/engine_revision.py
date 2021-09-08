from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import Field, PositiveInt

from firebolt.firebolt_client import get_firebolt_client
from firebolt.model import FireboltBaseModel
from firebolt.model.instance_type import (
    InstanceType,
    InstanceTypeKey,
    instance_types,
)


class EngineRevisionKey(FireboltBaseModel):
    account_id: str
    engine_id: str
    engine_revision_id: str


class EngineRevisionSpecification(FireboltBaseModel):
    """
    An EngineRevision Specification.

    Notably, it determines which instance types and how many of them its Engine gets.

    See Also: engine.Settings, which also contains engine configuration.
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

    @classmethod
    def ingest_default(
        cls,
        compute_instance_type_name: Optional[str] = None,
        compute_instance_count: Optional[int] = None,
    ) -> EngineRevisionSpecification:
        """
        Default EngineRevisionSpecification for data ingestion.

        Args:
            compute_instance_type_name: Name of the instance type to use for the Engine.
            compute_instance_count: Number of instances to use for the Engine.

        Returns:
            A default Specification, updated with any user-defined settings.
        """
        if compute_instance_type_name is None:
            compute_instance_type_name = "i3.4xlarge"
        if compute_instance_count is None:
            compute_instance_count = 2

        instance_type_key = instance_types.get_by_name(
            instance_type_name=compute_instance_type_name
        ).key
        return cls(
            db_compute_instances_type_key=instance_type_key,
            db_compute_instances_count=compute_instance_count,
            db_compute_instances_use_spot=False,
            db_version="",
            proxy_instances_type_key=instance_type_key,
            proxy_instances_count=1,
            proxy_version="",
        )

    @classmethod
    def analytics_default(
        cls,
        compute_instance_type_name: Optional[str] = None,
        compute_instance_count: Optional[int] = None,
    ) -> EngineRevisionSpecification:
        """
        Default EngineRevisionSpecification for analytics (querying).

        Args:
            compute_instance_type_name: Name of the instance type to use for the Engine.
            compute_instance_count: Number of instances to use for the Engine.

        Returns:
            A default Specification, updated with any user-defined settings.
        """
        if compute_instance_type_name is None:
            compute_instance_type_name = "m5d.4xlarge"
        if compute_instance_count is None:
            compute_instance_count = 1

        instance_type_key = instance_types.get_by_name(
            instance_type_name=compute_instance_type_name
        ).key
        return cls(
            db_compute_instances_type_key=instance_type_key,
            db_compute_instances_count=compute_instance_count,
            db_compute_instances_use_spot=False,
            db_version="",
            proxy_instances_type_key=instance_type_key,
            proxy_instances_count=1,
            proxy_version="",
        )


class EngineRevision(FireboltBaseModel):
    """
    A Firebolt Engine revision, which contains a Specification (instance types, counts).

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

    @classmethod
    def get_by_id(cls, engine_id: str, engine_revision_id: str) -> EngineRevision:
        """Get an EngineRevision from Firebolt by engine_id and engine_revision_id"""
        fc = get_firebolt_client()
        return cls.get_by_engine_revision_key(
            EngineRevisionKey(
                account_id=fc.account_id,
                engine_id=engine_id,
                engine_revision_id=engine_revision_id,
            )
        )

    @classmethod
    def get_by_engine_revision_key(
        cls, engine_revision_key: EngineRevisionKey
    ) -> EngineRevision:
        """
        Fetch an EngineRevision from Firebolt by it's key.

        Args:
            engine_revision_key: Key of the desired EngineRevision.

        Returns:
            The requested EngineRevision
        """
        fc = get_firebolt_client()
        response = fc.http_client.get(
            url=f"/core/v1/accounts/{engine_revision_key.account_id}"
            f"/engines/{engine_revision_key.engine_id}"
            f"/engineRevisions/{engine_revision_key.engine_revision_id}",
        )
        engine_spec: dict = response.json()["engine_revision"]
        return cls.parse_obj(engine_spec)

    @classmethod
    def analytics_default(
        cls,
        compute_instance_type_name: Optional[str] = None,
        compute_instance_count: Optional[int] = None,
    ) -> EngineRevision:
        """Create a local EngineRevision with default settings for analytics"""
        return cls(
            specification=EngineRevisionSpecification.analytics_default(
                compute_instance_type_name=compute_instance_type_name,
                compute_instance_count=compute_instance_count,
            )
        )

    @classmethod
    def ingest_default(
        cls,
        compute_instance_type_name: Optional[str] = None,
        compute_instance_count: Optional[int] = None,
    ) -> EngineRevision:
        """Create a local EngineRevision with default settings for ingestion"""
        return cls(
            specification=EngineRevisionSpecification.ingest_default(
                compute_instance_type_name=compute_instance_type_name,
                compute_instance_count=compute_instance_count,
            )
        )

    @property
    def compute_instance_type(self) -> InstanceType:
        return instance_types.get_by_key(
            instance_type_key=self.specification.db_compute_instances_type_key
        )

    @property
    def compute_instance_type_name(self) -> str:
        return self.compute_instance_type.name
