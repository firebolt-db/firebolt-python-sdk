import os
from typing import Optional


class EnvVar:
    def __init__(self, key: str):
        self.key = key
        self._value = os.getenv(key=self.key)

    def __repr__(self) -> str:
        return f"RequiredEnvVar(key={self.key}, value={self._value})"

    def get_required_value(self) -> str:
        self._value = os.getenv(key=self.key)
        if self._value is None:
            raise ValueError(
                f"Environment variable {self.key} is required, but is not set!"
            )
        return self._value

    def get_optional_value(self) -> Optional[str]:
        self._value = os.getenv(key=self.key)
        return self._value


# required
FIREBOLT_SERVER = EnvVar("FIREBOLT_SERVER")
FIREBOLT_USER = EnvVar("FIREBOLT_USER")
FIREBOLT_PASSWORD = EnvVar("FIREBOLT_PASSWORD")

# optional
FIREBOLT_DEFAULT_REGION = EnvVar("FIREBOLT_DEFAULT_REGION")
FIREBOLT_DEFAULT_PROVIDER = EnvVar("FIREBOLT_DEFAULT_PROVIDER")
