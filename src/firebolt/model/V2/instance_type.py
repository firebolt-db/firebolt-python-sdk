from enum import Enum
from typing import Any


class InstanceType(Enum):
    S = "S"
    M = "M"
    L = "L"
    XL = "XL"
    UNKNOWN = "UNKNOWN"  # instance type could not be determined

    @classmethod
    def _missing_(cls, value: Any) -> "InstanceType":
        return cls.UNKNOWN
