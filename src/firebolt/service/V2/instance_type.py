from typing import List, Optional

from firebolt.model.V2.instance_type import InstanceType
from firebolt.service.V2.base import BaseService


class InstanceTypeService(BaseService):
    @staticmethod
    def instance_types() -> List[InstanceType]:
        return [it for it in InstanceType]

    @staticmethod
    def cheapest_instance() -> Optional[InstanceType]:
        return InstanceType.S

    @staticmethod
    def get(name: str) -> Optional[InstanceType]:
        """
        Get an instance type by name.

        Args:
            name: Name of the instance (eg. "i3.4xlarge")

        Returns:
            The requested instance type or None if it wasn't found
        """
        try:
            return InstanceType(name)
        except ValueError:
            return None
