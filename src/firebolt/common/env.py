import os


class EnvVar:
    def __init__(self, key: str):
        self.key = key
        self._value = os.getenv(key=self.key)

    def __repr__(self):
        return f"RequiredEnvVar(key={self.key}, value={self._value})"

    def get_value(self, is_required=True):
        self._value = os.getenv(key=self.key)
        if is_required and not self._value:
            raise ValueError(
                f"Environment variable {self.key} is required, but is not set!"
            )
        return self._value


FIREBOLT_SERVER = EnvVar("FIREBOLT_SERVER")
FIREBOLT_USER = EnvVar("FIREBOLT_USER")
FIREBOLT_PASSWORD = EnvVar("FIREBOLT_PASSWORD")
