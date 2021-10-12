from enum import Enum
from types import DynamicClassAttribute


class EngineType(Enum):
    GENERAL_PURPOSE = "GENERAL_PURPOSE"
    DATA_ANALYTICS = "DATA_ANALYTICS"

    @DynamicClassAttribute
    def api_settings_preset_name(self) -> str:
        return {
            EngineType.GENERAL_PURPOSE: "ENGINE_SETTINGS_PRESET_GENERAL_PURPOSE",
            EngineType.DATA_ANALYTICS: "ENGINE_SETTINGS_PRESET_DATA_ANALYTICS",
        }[self]


class WarmupMethod(Enum):
    MINIMAL = "MINIMAL"
    PRELOAD_INDEXES = "PRELOAD_INDEXES"
    PRELOAD_ALL_DATA = "PRELOAD_ALL_DATA"

    @DynamicClassAttribute
    def api_name(self) -> str:
        return {
            WarmupMethod.MINIMAL: "ENGINE_SETTINGS_WARM_UP_MINIMAL",
            WarmupMethod.PRELOAD_INDEXES: "ENGINE_SETTINGS_WARM_UP_INDEXES",
            WarmupMethod.PRELOAD_ALL_DATA: "ENGINE_SETTINGS_WARM_UP_ALL",
        }[self]
