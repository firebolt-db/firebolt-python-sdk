from typing import Optional

from firebolt.model.engine_revision import (
    EngineRevision,
    EngineRevisionKey,
    EngineRevisionSpecification,
)
from firebolt.service.base import BaseService


class EngineRevisionService(BaseService):
    def get_engine_revision_by_id(
        self, engine_id: str, engine_revision_id: str
    ) -> EngineRevision:
        """Get an EngineRevision from Firebolt by engine_id and engine_revision_id."""
        return self.get_engine_revision_by_key(
            EngineRevisionKey(
                account_id=self.account_id,
                engine_id=engine_id,
                engine_revision_id=engine_revision_id,
            )
        )

    def get_engine_revision_by_key(
        self, engine_revision_key: EngineRevisionKey
    ) -> EngineRevision:
        """
        Fetch an EngineRevision from Firebolt by it's key.

        Args:
            engine_revision_key: Key of the desired EngineRevision.

        Returns:
            The requested EngineRevision
        """
        response = self.client.get(
            url=f"/core/v1/accounts/{engine_revision_key.account_id}"
            f"/engines/{engine_revision_key.engine_id}"
            f"/engineRevisions/{engine_revision_key.engine_revision_id}",
        )
        engine_spec: dict = response.json()["engine_revision"]
        return EngineRevision.parse_obj(engine_spec)

    def create_analytics_engine_revision(
        self,
        compute_instance_type_name: Optional[str] = None,
        compute_instance_count: Optional[int] = None,
    ) -> EngineRevision:
        """Create a local EngineRevision with default settings for analytics."""
        return EngineRevision(
            specification=self.create_analytics_engine_revision_specification(
                compute_instance_type_name=compute_instance_type_name,
                compute_instance_count=compute_instance_count,
            )
        )

    def create_general_purpose_engine_revision(
        self,
        compute_instance_type_name: Optional[str] = None,
        compute_instance_count: Optional[int] = None,
    ) -> EngineRevision:
        """
        Create a local EngineRevision with default settings for
        general purpose usage / data ingestion.
        """
        return EngineRevision(
            specification=self.create_general_purpose_engine_revision_specification(
                compute_instance_type_name=compute_instance_type_name,
                compute_instance_count=compute_instance_count,
            )
        )

    def create_analytics_engine_revision_specification(
        self,
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

        instance_type_key = self.resource_manager.instance_types.get_by_name(
            instance_type_name=compute_instance_type_name
        ).key
        return EngineRevisionSpecification(
            db_compute_instances_type_key=instance_type_key,
            db_compute_instances_count=compute_instance_count,
            db_compute_instances_use_spot=False,
            db_version="",
            proxy_instances_type_key=instance_type_key,
            proxy_instances_count=1,
            proxy_version="",
        )

    def create_general_purpose_engine_revision_specification(
        self,
        compute_instance_type_name: Optional[str] = None,
        compute_instance_count: Optional[int] = None,
    ) -> EngineRevisionSpecification:
        """
        Default EngineRevisionSpecification for general purpose / data ingestion.

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

        instance_type_key = self.resource_manager.instance_types.get_by_name(
            instance_type_name=compute_instance_type_name
        ).key
        return EngineRevisionSpecification(
            db_compute_instances_type_key=instance_type_key,
            db_compute_instances_count=compute_instance_count,
            db_compute_instances_use_spot=False,
            db_version="",
            proxy_instances_type_key=instance_type_key,
            proxy_instances_count=1,
            proxy_version="",
        )
