from typing import List, Optional

from firebolt.model.V2.instance_type import InstanceType
from firebolt.service.V2.base import BaseService
from firebolt.utils.exception import InstanceTypeNotFoundError
from firebolt.utils.urls import ACCOUNT_INSTANCE_TYPES_URL
from firebolt.utils.util import cached_property


class InstanceTypeService(BaseService):
    @cached_property
    def instance_types(self) -> List[InstanceType]:
        """List of instance types available on Firebolt."""

        response = self.client.get(
            url=ACCOUNT_INSTANCE_TYPES_URL.format(account_id=self.account_id),
            params={"page.first": 5000},
        )

        # Only take one instance type with a specific name
        instance_types, names = list(), set()
        for it in [i["node"] for i in response.json()["edges"]]:
            if it["name"] not in names:
                names.add(it["name"])
                instance_types.append(InstanceType._from_dict(it, self))
        return instance_types

    @cached_property
    def cheapest_instance(self) -> Optional[InstanceType]:
        # Get only available instances in region
        if not self.instance_types:
            return None

        return min(
            self.instance_types,
            key=lambda x: x.price_per_hour_cents
            if x.price_per_hour_cents
            else float("Inf"),
        )

    def get(self, name: str) -> InstanceType:
        """
        Get an instance type by name.

        Args:
            name: Name of the instance (eg. "i3.4xlarge")

        Returns:
            The requested instance type or None if it wasn't found
        """

        # Will raise an error if neither set
        its = [it for it in self.instance_types if it.name == name]
        if len(its) == 0:
            raise InstanceTypeNotFoundError(name)
        return its[0]
